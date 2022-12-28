#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import math
import multiprocessing
import os
import platform
import queue
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict
from typing import Any

import dill
import psutil
import zmq
from funcx_common.messagepack.message_types import TaskTransition
from funcx_common.tasks import ActorName, TaskState
from parsl.version import VERSION as PARSL_VERSION

from funcx.serialize import FuncXSerializer
from funcx_endpoint.exception_handling import get_error_string, get_result_error_details
from funcx_endpoint.executors.high_throughput.container_sched import naive_scheduler
from funcx_endpoint.executors.high_throughput.mac_safe_queue import mpQueue
from funcx_endpoint.executors.high_throughput.messages import (
    ManagerStatusReport,
    Message,
    Task,
)
from funcx_endpoint.executors.high_throughput.worker_map import WorkerMap
from funcx_endpoint.logging_config import FXLogger, setup_logging

RESULT_TAG = 10
TASK_REQUEST_TAG = 11
HEARTBEAT_CODE = (2**32) - 1

log: FXLogger = logging.getLogger(__name__)  # type: ignore


class TaskCancelled(Exception):
    """Task is cancelled by user request."""

    def __init__(self, worker_id, manager_id):
        self.worker_id = worker_id
        self.manager_id = manager_id
        self.tstamp = time.time()

    def __str__(self):
        return (
            "Task cancelled based on user request on manager: "
            f"{self.manager_id}, worker: {self.worker_id}"
        )


class Manager:
    """Manager manages task execution by the workers

                |         0mq              |    Manager         |   Worker Processes
                |                          |                    |
                | <-----Request N task-----+--Count task reqs   |      Request task<--+
    Interchange | -------------------------+->Receive task batch|          |          |
                |                          |  Distribute tasks--+----> Get(block) &   |
                |                          |                    |      Execute task   |
                |                          |                    |          |          |
                | <------------------------+--Return results----+----  Post result    |
                |                          |                    |          |          |
                |                          |                    |          +----------+
                |                          |                IPC-Qeueues

    """

    def __init__(
        self,
        task_q_url="tcp://127.0.0.1:50097",
        result_q_url="tcp://127.0.0.1:50098",
        max_queue_size=10,
        cores_per_worker=1,
        available_accelerators: list[str] | None = None,
        max_workers=float("inf"),
        uid=None,
        heartbeat_threshold=120,
        heartbeat_period=30,
        logdir=None,
        debug=False,
        block_id=None,
        internal_worker_port_range=(50000, 60000),
        worker_mode="singularity_reuse",
        container_cmd_options="",
        scheduler_mode="hard",
        worker_type=None,
        worker_max_idletime=60,
        # TODO : This should be 10ms
        poll_period=100,
    ):
        """
        Parameters
        ----------
        worker_url : str
             Worker url on which workers will attempt to connect back

        uid : str
             string unique identifier

        cores_per_worker : float
             cores to be assigned to each worker. Oversubscription is possible
             by setting cores_per_worker < 1.0. Default=1

        available_accelerators: list of strings
            Accelerators available for workers to use.
            default: empty list

        max_workers : int
             caps the maximum number of workers that can be launched.
             default: infinity

        heartbeat_threshold : int
             Seconds since the last message from the interchange after which the
             interchange is assumed to be un-available, and the manager initiates
             shutdown. Default:120s

             Number of seconds since the last message from the interchange after which
             the worker assumes that the interchange is lost and the manager shuts down.
             Default:120

        heartbeat_period : int
             Number of seconds after which a heartbeat message is sent to the
             interchange

        internal_worker_port_range : tuple(int, int)
             Port range from which the port(s) for the workers to connect to the manager
             is picked.
             Default: (50000,60000)

        worker_mode : str
             Pick between 3 supported modes for the worker:
              1. no_container : Worker launched without containers
              2. singularity_reuse : Worker launched inside a singularity container that
                                     will be reused
              3. singularity_single_use : Each worker and task runs inside a new
                                          container instance.

        container_cmd_options: str
              Container command strings to be added to associated container command.
              For example, singularity exec {container_cmd_options}

        scheduler_mode : str
             Pick between 2 supported modes for the manager:
              1. hard: the manager cannot change the launched container type
              2. soft: the manager can decide whether to launch different containers

        worker_type : str
             If set, the worker type for this manager is fixed. Default: None

        poll_period : int
             Timeout period used by the manager in milliseconds. Default: 10ms
        """
        log.info("Manager started")

        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.setsockopt(zmq.IDENTITY, uid.encode("utf-8"))
        # Linger is set to 0, so that the manager can exit even when there might be
        # messages in the pipe
        self.task_incoming.setsockopt(zmq.LINGER, 0)
        self.task_incoming.connect(task_q_url)

        self.logdir = logdir
        self.debug = debug
        self.block_id = block_id
        self.result_outgoing = self.context.socket(zmq.DEALER)
        self.result_outgoing.setsockopt(zmq.IDENTITY, uid.encode("utf-8"))
        self.result_outgoing.setsockopt(zmq.LINGER, 0)
        self.result_outgoing.connect(result_q_url)

        log.info("Manager connected")

        self.uid = uid

        self.worker_mode = worker_mode
        self.container_cmd_options = container_cmd_options
        self.scheduler_mode = scheduler_mode
        self.worker_type = worker_type
        self.worker_max_idletime = worker_max_idletime
        self.cores_on_node = multiprocessing.cpu_count()
        self.max_workers = max_workers
        self.cores_per_workers = cores_per_worker
        self.available_mem_on_node = round(
            psutil.virtual_memory().available / (2**30), 1
        )
        self.max_worker_count = min(
            max_workers, math.floor(self.cores_on_node / cores_per_worker)
        )

        # Control pinning to accelerators
        self.available_accelerators = available_accelerators or []
        if self.available_accelerators:
            self.max_worker_count = min(
                self.max_worker_count, len(self.available_accelerators)
            )

        self.worker_map = WorkerMap(self.max_worker_count, self.available_accelerators)

        self.internal_worker_port_range = internal_worker_port_range

        self.funcx_task_socket = self.context.socket(zmq.ROUTER)
        self.funcx_task_socket.set_hwm(0)
        self.address = "127.0.0.1"
        self.worker_port = self.funcx_task_socket.bind_to_random_port(
            "tcp://*",
            min_port=self.internal_worker_port_range[0],
            max_port=self.internal_worker_port_range[1],
        )

        log.info(
            "Manager listening on {} port for incoming worker connections".format(
                self.worker_port
            )
        )

        self.task_queues: dict[str, queue.Queue] = {}
        if worker_type:
            self.task_queues[worker_type] = queue.Queue()
        self.outstanding_task_count: dict[str, int] = {}
        self.task_type_mapping: dict[str, str] = {}

        self.pending_result_queue = mpQueue()

        self.max_queue_size = max_queue_size + self.max_worker_count
        self.tasks_per_round = 1

        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.poll_period = poll_period
        self.serializer = FuncXSerializer()
        self.next_worker_q: list[str] = []  # FIFO queue for spinning up workers.
        self.worker_procs: dict[str, subprocess.Popen] = {}

        self.task_status_deltas: dict[str, list[TaskTransition]] = defaultdict(list)

        self._kill_event = threading.Event()
        self._result_pusher_thread = threading.Thread(
            target=self.push_results, args=(self._kill_event,), name="Result-Pusher"
        )
        self._status_report_thread = threading.Thread(
            target=self._status_report_loop,
            args=(self._kill_event,),
            name="Status-Report",
        )
        self.container_switch_count = 0

        self.poller = zmq.Poller()
        self.poller.register(self.task_incoming, zmq.POLLIN)
        self.poller.register(self.funcx_task_socket, zmq.POLLIN)
        self.task_worker_map: dict[str, Any] = {}

        self.task_done_counter = 0
        self.task_finalization_lock = threading.Lock()

    def create_reg_message(self):
        """Creates a registration message to identify the worker to the interchange"""
        msg = {
            "parsl_v": PARSL_VERSION,
            "python_v": "{}.{}.{}".format(
                sys.version_info.major, sys.version_info.minor, sys.version_info.micro
            ),
            "max_worker_count": self.max_worker_count,
            "cores": self.cores_on_node,
            "mem": self.available_mem_on_node,
            "block_id": self.block_id,
            "worker_type": self.worker_type,
            "os": platform.system(),
            "hname": platform.node(),
            "dir": os.getcwd(),
        }
        b_msg = json.dumps(msg).encode("utf-8")
        return b_msg

    def pull_tasks(self, kill_event):
        """Pull tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue


        While :
            receive results and task requests from the workers
            receive tasks/heartbeats from the Interchange
            match tasks to workers
            if task doesn't have appropriate worker type:
                 launch worker of type.. with LRU or some sort of caching strategy.
            if workers >> tasks:
                 advertize available capacity

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        log.info("starting")

        # Send a registration message
        msg = self.create_reg_message()
        log.debug(f"Sending registration message: {msg}")
        self.task_incoming.send(msg)
        last_interchange_contact = time.time()
        task_recv_counter = 0

        poll_timer = self.poll_period

        new_worker_map = None
        while not kill_event.is_set():
            # Disabling the check on ready_worker_queue disables batching
            log.trace("Loop start")
            pending_task_count = task_recv_counter - self.task_done_counter
            ready_worker_count = self.worker_map.ready_worker_count()
            log.trace(
                "pending_task_count: %s, Ready_worker_count: %s",
                pending_task_count,
                ready_worker_count,
            )

            if pending_task_count < self.max_queue_size and ready_worker_count > 0:
                ads = self.worker_map.advertisement()
                log.trace("Requesting tasks: %s", ads)
                msg = dill.dumps(ads)
                self.task_incoming.send(msg)

            # Receive results from the workers, if any
            socks = dict(self.poller.poll(timeout=poll_timer))

            if (
                self.funcx_task_socket in socks
                and socks[self.funcx_task_socket] == zmq.POLLIN
            ):
                self.poll_funcx_task_socket()

            # Receive task batches from Interchange and forward to workers
            if self.task_incoming in socks and socks[self.task_incoming] == zmq.POLLIN:

                # If we want to wrap the task_incoming polling into a separate function,
                # we need to
                #   self.poll_task_incoming(
                #       poll_timer,
                #       last_interchange_contact,
                #       kill_event,
                #       task_revc_counter
                #   )
                poll_timer = 0
                _, pkl_msg = self.task_incoming.recv_multipart()
                message = dill.loads(pkl_msg)
                last_interchange_contact = time.time()

                if message == "STOP":
                    log.critical("Received stop request")
                    kill_event.set()
                    break

                elif type(message) == tuple and message[0] == "TASK_CANCEL":
                    with self.task_finalization_lock:
                        task_id = message[1]
                        log.info(f"Received TASK_CANCEL request for task: {task_id}")
                        if task_id not in self.task_worker_map:
                            log.warning(f"Task:{task_id} is not in task_worker_map.")
                            log.warning("Possible duplicate cancel or race-condition")
                            continue
                        # Cancel task by killing the worker it is on
                        worker_id_raw = self.task_worker_map[task_id]["worker_id"]
                        worker_to_kill = self.task_worker_map[task_id][
                            "worker_id"
                        ].decode("utf-8")
                        worker_type = self.task_worker_map[task_id]["task_type"]
                        log.debug(
                            "Cancelling task running on worker: %s",
                            self.task_worker_map[task_id],
                        )
                        try:
                            log.info(f"Removing worker:{worker_id_raw} from map")
                            self.worker_map.start_remove_worker(worker_type)

                            log.info(
                                f"Popping worker:{worker_to_kill} from worker_procs"
                            )
                            proc = self.worker_procs.pop(worker_to_kill)
                            log.warning(f"Sending process:{proc.pid} terminate signal")
                            proc.terminate()
                            try:
                                proc.wait(1)  # Wait 1 second before attempting SIGKILL
                            except subprocess.TimeoutExpired:
                                log.exception("Process did not terminate in 1 second")
                                log.warning(f"Sending process:{proc.pid} kill signal")
                                proc.kill()
                            else:
                                log.debug(
                                    f"Worker process exited with : {proc.returncode}"
                                )

                            # Now that the worker is dead, remove it from worker map
                            self.worker_map.remove_worker(worker_id_raw)
                            raise TaskCancelled(worker_to_kill, self.uid)
                        except Exception as e:
                            log.exception(f"Raise exception, handling: {e}")
                            result_package = {
                                "task_id": task_id,
                                "container_id": worker_type,
                                "error_details": get_result_error_details(e),
                                "exception": get_error_string(tb_levels=0),
                            }
                            self.pending_result_queue.put(dill.dumps(result_package))

                        worker_proc = self.worker_map.add_worker(
                            worker_id=str(self.worker_map.worker_id_counter),
                            worker_type=self.worker_type,
                            container_cmd_options=self.container_cmd_options,
                            address=self.address,
                            debug=self.debug,
                            uid=self.uid,
                            logdir=self.logdir,
                            worker_port=self.worker_port,
                        )
                        self.worker_procs.update(worker_proc)
                        self.task_worker_map.pop(task_id)
                        self.remove_task(task_id)

                elif message == HEARTBEAT_CODE:
                    log.debug("Got heartbeat from interchange")

                else:
                    tasks = [
                        (rt["local_container"], Message.unpack(rt["raw_buffer"]))
                        for rt in message
                    ]

                    task_recv_counter += len(tasks)
                    log.debug(
                        "Got tasks: {} of {}".format(
                            [t[1].task_id for t in tasks], task_recv_counter
                        )
                    )

                    for task_type, task in tasks:
                        log.debug(f"Task is of type: {task_type}")

                        if task_type not in self.task_queues:
                            self.task_queues[task_type] = queue.Queue()
                        if task_type not in self.outstanding_task_count:
                            self.outstanding_task_count[task_type] = 0
                        self.task_queues[task_type].put(task)
                        self.outstanding_task_count[task_type] += 1
                        self.task_type_mapping[task.task_id] = task_type
                        log.debug(
                            "Got task: Outstanding task counts: {}".format(
                                self.outstanding_task_count
                            )
                        )
                        log.debug(
                            f"Task {task.task_id} pushed to task queue "
                            f"for type: {task_type}"
                        )

            else:
                log.trace("No incoming tasks")
                # Limit poll duration to heartbeat_period
                # heartbeat_period is in s vs poll_timer in ms
                if not poll_timer:
                    poll_timer = self.poll_period
                poll_timer = min(self.heartbeat_period * 1000, poll_timer * 2)

                # Only check if no messages were received.
                if time.time() > last_interchange_contact + self.heartbeat_threshold:
                    log.critical(
                        "Missing contact with interchange beyond heartbeat_threshold"
                    )
                    kill_event.set()
                    log.critical("Killing all workers")
                    for proc in self.worker_procs.values():
                        proc.kill()
                    log.critical("Exiting")
                    break

            log.trace(
                "To-Die Counts: %s, alive worker counts: %s",
                self.worker_map.to_die_count,
                self.worker_map.total_worker_type_counts,
            )

            new_worker_map = naive_scheduler(
                self.task_queues,
                self.outstanding_task_count,
                self.max_worker_count,
                new_worker_map,
                self.worker_map.to_die_count,
            )
            log.trace("New worker map: %s", new_worker_map)

            # NOTE: Wipes the queue -- previous scheduling loops don't affect what's
            # needed now.
            self.next_worker_q, need_more = self.worker_map.get_next_worker_q(
                new_worker_map
            )

            # Spin up any new workers according to the worker queue.
            # Returns the total number of containers that have spun up.
            self.worker_procs.update(
                self.worker_map.spin_up_workers(
                    self.next_worker_q,
                    mode=self.worker_mode,
                    debug=self.debug,
                    container_cmd_options=self.container_cmd_options,
                    address=self.address,
                    uid=self.uid,
                    logdir=self.logdir,
                    worker_port=self.worker_port,
                )
            )
            log.trace("Worker processes: %s", self.worker_procs)

            #  Count the workers of each type that need to be removed
            spin_downs, container_switch_count = self.worker_map.spin_down_workers(
                new_worker_map,
                worker_max_idletime=self.worker_max_idletime,
                need_more=need_more,
                scheduler_mode=self.scheduler_mode,
            )
            self.container_switch_count += container_switch_count
            log.trace(
                "Container switch count: total %s, cur %s",
                self.container_switch_count,
                container_switch_count,
            )

            for w_type in spin_downs:
                self.remove_worker_init(w_type)

            current_worker_map = self.worker_map.get_worker_counts()
            for task_type in current_worker_map:
                if task_type == "unused":
                    continue

                # *** Match tasks to workers *** #
                else:
                    available_workers = current_worker_map[task_type]
                    log.trace(
                        "Available workers of type %s: %s", task_type, available_workers
                    )

                    for _i in range(available_workers):
                        if (
                            task_type in self.task_queues
                            and not self.task_queues[task_type].qsize() == 0
                            and not self.worker_map.worker_queues[task_type].qsize()
                            == 0
                        ):

                            log.debug(
                                "Task type {} has task queue size {}".format(
                                    task_type, self.task_queues[task_type].qsize()
                                )
                            )
                            log.debug(
                                "... and available workers: {}".format(
                                    self.worker_map.worker_queues[task_type].qsize()
                                )
                            )

                            self.send_task_to_worker(task_type)

    def poll_funcx_task_socket(self, test=False):
        try:
            w_id, m_type, message = self.funcx_task_socket.recv_multipart()
            if m_type == b"REGISTER":
                reg_info = dill.loads(message)
                log.debug(f"Registration received from worker:{w_id} {reg_info}")
                self.worker_map.register_worker(w_id, reg_info["worker_type"])

            elif m_type == b"TASK_RET":
                # the following steps are also shared by task_cancel
                with self.task_finalization_lock:
                    log.debug(f"Result received from worker: {w_id}")
                    task_id = dill.loads(message)["task_id"]
                    try:
                        self.remove_task(task_id)
                    except KeyError:
                        log.exception(f"Task:{task_id} missing in task structure")
                    else:
                        self.pending_result_queue.put(message)
                        self.worker_map.put_worker(w_id)

            elif m_type == b"WRKR_DIE":
                log.debug(f"[WORKER_REMOVE] Removing worker {w_id} from worker_map...")
                log.debug(
                    "Ready worker counts: {}".format(
                        self.worker_map.ready_worker_type_counts
                    )
                )
                log.debug(
                    "Total worker counts: {}".format(
                        self.worker_map.total_worker_type_counts
                    )
                )
                self.worker_map.remove_worker(w_id)
                proc = self.worker_procs.pop(w_id.decode())
                if not proc.poll():
                    try:
                        proc.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        log.warning(
                            "[WORKER_REMOVE] Timeout waiting for worker %s process to "
                            "terminate",
                            w_id,
                        )
                log.debug(f"[WORKER_REMOVE] Removing worker {w_id} process object")
                log.debug(f"[WORKER_REMOVE] Worker processes: {self.worker_procs}")

            if test:
                return dill.loads(message)

        except Exception:
            log.exception("Unhandled exception while processing worker messages")

    def remove_task(self, task_id: str):
        task_type = self.task_type_mapping.pop(task_id)
        self.task_status_deltas.pop(task_id, None)
        self.outstanding_task_count[task_type] -= 1
        self.task_done_counter += 1

    def send_task_to_worker(self, task_type):
        task = self.task_queues[task_type].get()
        worker_id = self.worker_map.get_worker(task_type)

        log.debug(f"Sending task {task.task_id} to {worker_id}")
        # TODO: Some duplication of work could be avoided here
        to_send = [
            worker_id,
            dill.dumps(task.task_id),
            dill.dumps(task.container_id),
            task.pack(),
        ]
        self.funcx_task_socket.send_multipart(to_send)
        self.worker_map.update_worker_idle(task_type)
        if task.task_id != "KILL":
            log.debug(f"Set task {task.task_id} to RUNNING")
            tt = TaskTransition(
                timestamp=time.time_ns(),
                state=TaskState.RUNNING,
                actor=ActorName.MANAGER,
            )
            self.task_status_deltas[task.task_id].append(tt)
            self.task_worker_map[task.task_id] = {
                "worker_id": worker_id,
                "task_type": task_type,
            }
        log.debug("Sending complete")

    def _status_report_loop(self, kill_event: threading.Event):
        log.debug("Manager status reporting loop starting")

        while not kill_event.wait(timeout=self.heartbeat_period):
            msg = ManagerStatusReport(
                self.task_status_deltas,
                self.container_switch_count,
            )
            log.info(f"Sending status report to interchange: {msg.task_statuses}")
            self.pending_result_queue.put(msg)
            log.info("Clearing task deltas")
            self.task_status_deltas.clear()

    def push_results(self, kill_event, max_result_batch_size=1):
        """Listens on the pending_result_queue and sends out results via 0mq

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """

        log.debug("Starting thread")

        push_poll_period = (
            max(10, self.poll_period) / 1000
        )  # push_poll_period must be atleast 10 ms
        log.debug(f"push poll period: {push_poll_period}")

        last_beat = time.time()
        items = []

        while not kill_event.is_set():
            try:
                r = self.pending_result_queue.get(block=True, timeout=push_poll_period)
                # This avoids the interchange searching and attempting to unpack every
                # message in case it's a status report.
                # (It would be better to use Task Messages eventually to make this more
                #  uniform)
                # TODO: use task messages, and don't have to prepend
                if isinstance(r, ManagerStatusReport):
                    items.insert(0, r.pack())
                else:
                    items.append(r)
            except queue.Empty:
                pass
            except Exception as e:
                log.exception(f"Got an exception: {e}")

            # If we have reached poll_period duration or timer has expired, we send
            # results
            if (
                len(items) >= self.max_queue_size
                or time.time() > last_beat + push_poll_period
            ):
                last_beat = time.time()
                if items:
                    self.result_outgoing.send_multipart(items)
                    items = []

        log.critical("Exiting")

    def remove_worker_init(self, worker_type):
        """
        Kill/Remove a worker of a given worker_type.

        Add a kill message to the task_type queue.

        Assumption : All workers of the same type are uniform, and therefore don't
                     discriminate when killing.
        """

        log.debug(
            "[WORKER_REMOVE] Appending KILL message to worker queue {}".format(
                worker_type
            )
        )
        self.worker_map.start_remove_worker(worker_type)
        task = Task(task_id="KILL", container_id="RAW", task_buffer="KILL")
        self.task_queues[worker_type].put(task)

    def start(self):
        """
        * while True:
            Receive tasks and start appropriate workers
            Push tasks to available workers
            Forward results
        """

        if self.worker_type and self.scheduler_mode == "hard":
            log.debug(
                "[MANAGER] Start an initial worker with worker type {}".format(
                    self.worker_type
                )
            )
            self.worker_procs.update(
                self.worker_map.add_worker(
                    worker_id=str(self.worker_map.worker_id_counter),
                    worker_type=self.worker_type,
                    container_cmd_options=self.container_cmd_options,
                    address=self.address,
                    debug=self.debug,
                    uid=self.uid,
                    logdir=self.logdir,
                    worker_port=self.worker_port,
                )
            )

        log.debug("Initial workers launched")
        self._result_pusher_thread.start()
        self._status_report_thread.start()
        self.pull_tasks(self._kill_event)
        log.info("Waiting")


def cli_run():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Count of apps to launch"
    )
    parser.add_argument(
        "-l",
        "--logdir",
        default="process_worker_pool_logs",
        help="Process worker pool log directory",
    )
    parser.add_argument(
        "-u",
        "--uid",
        default=str(uuid.uuid4()).split("-")[-1],
        help="Unique identifier string for Manager",
    )
    parser.add_argument(
        "-b", "--block_id", default=None, help="Block identifier string for Manager"
    )
    parser.add_argument(
        "-c",
        "--cores_per_worker",
        default="1.0",
        help="Number of cores assigned to each worker process. Default=1.0",
    )
    parser.add_argument(
        "-a",
        "--available-accelerators",
        default=(),
        nargs="*",
        help="List of available accelerators",
    )
    parser.add_argument(
        "-t", "--task_url", required=True, help="REQUIRED: ZMQ url for receiving tasks"
    )
    parser.add_argument(
        "--max_workers",
        default=float("inf"),
        help="Caps the maximum workers that can be launched, default:infinity",
    )
    parser.add_argument(
        "--hb_period",
        default=30,
        help="Heartbeat period in seconds. Uses manager default unless set",
    )
    parser.add_argument(
        "--hb_threshold",
        default=120,
        help="Heartbeat threshold in seconds. Uses manager default unless set",
    )
    parser.add_argument("--poll", default=10, help="Poll period used in milliseconds")
    parser.add_argument(
        "--worker_type", default=None, help="Fixed worker type of manager"
    )
    parser.add_argument(
        "--worker_mode",
        default="singularity_reuse",
        help=(
            "Choose the mode of operation from "
            "(no_container, singularity_reuse, singularity_single_use"
        ),
    )
    parser.add_argument(
        "--container_cmd_options",
        default="",
        help=("Container cmd options to add to container startup cmd"),
    )
    parser.add_argument(
        "--scheduler_mode",
        default="soft",
        help=("Choose the mode of scheduler (hard, soft"),
    )
    parser.add_argument(
        "-r",
        "--result_url",
        required=True,
        help="REQUIRED: ZMQ url for posting results",
    )

    args = parser.parse_args()

    setup_logging(
        logfile=os.path.join(args.logdir, args.uid, "manager.log"), debug=args.debug
    )

    try:
        log.info(f"Python version: {sys.version}")
        log.info(f"Debug logging: {args.debug}")
        log.info(f"Log dir: {args.logdir}")
        log.info(f"Manager ID: {args.uid}")
        log.info(f"Block ID: {args.block_id}")
        log.info(f"cores_per_worker: {args.cores_per_worker}")
        log.info(f"available_accelerators: {args.available_accelerators}")
        log.info(f"task_url: {args.task_url}")
        log.info(f"result_url: {args.result_url}")
        log.info(f"hb_period: {args.hb_period}")
        log.info(f"hb_threshold: {args.hb_threshold}")
        log.info(f"max_workers: {args.max_workers}")
        log.info(f"poll_period: {args.poll}")
        log.info(f"worker_mode: {args.worker_mode}")
        log.info(f"container_cmd_options: {args.container_cmd_options}")
        log.info(f"scheduler_mode: {args.scheduler_mode}")
        log.info(f"worker_type: {args.worker_type}")

        manager = Manager(
            task_q_url=args.task_url,
            result_q_url=args.result_url,
            uid=args.uid,
            block_id=args.block_id,
            cores_per_worker=float(args.cores_per_worker),
            available_accelerators=args.available_accelerators,
            max_workers=args.max_workers
            if args.max_workers == float("inf")
            else int(args.max_workers),
            heartbeat_threshold=int(args.hb_threshold),
            heartbeat_period=int(args.hb_period),
            logdir=args.logdir,
            debug=args.debug,
            worker_mode=args.worker_mode,
            container_cmd_options=args.container_cmd_options,
            scheduler_mode=args.scheduler_mode,
            worker_type=args.worker_type,
            poll_period=int(args.poll),
        )
        manager.start()

    except Exception as e:
        log.critical("process_worker_pool exiting from an exception")
        log.exception(f"Caught error: {e}")
        raise
    else:
        log.info("process_worker_pool main event loop exiting normally")
        print("PROCESS_WORKER_POOL main event loop exiting normally")


if __name__ == "__main__":
    cli_run()
