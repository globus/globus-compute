from __future__ import annotations

import argparse
import collections
import copy
import json
import logging
import os
import platform
import queue
import signal
import sys
import threading
import time
import typing as t
from collections import defaultdict

import daemon
import dill
import zmq
from globus_compute_common.messagepack.message_types import TaskTransition
from globus_compute_common.tasks import ActorName, TaskState
from globus_compute_endpoint.engines.high_throughput.interchange_task_dispatch import (  # noqa: E501
    naive_interchange_task_dispatch,
)
from globus_compute_endpoint.engines.high_throughput.messages import (
    BadCommand,
    EPStatusReport,
    Heartbeat,
    Message,
    MessageType,
)
from globus_compute_endpoint.exception_handling import (
    get_error_string,
    get_result_error_details,
)
from globus_compute_endpoint.logging_config import ComputeLogger
from globus_compute_sdk.sdk.utils import chunk_by
from globus_compute_sdk.serialize import ComputeSerializer
from parsl.version import VERSION as PARSL_VERSION

if t.TYPE_CHECKING:
    import multiprocessing as mp

log: ComputeLogger = logging.getLogger(__name__)  # type: ignore

HEARTBEAT_CODE = (2**32) - 1
PKL_HEARTBEAT_CODE = dill.dumps(HEARTBEAT_CODE)


class ManagerLost(Exception):
    """Task lost due to worker loss. Worker is considered lost when multiple heartbeats
    have been missed.
    """

    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.tstamp = time.time()

    def __repr__(self):
        return f"Task failure due to loss of manager {self.worker_id}"

    def __str__(self):
        return self.__repr__()


class BadRegistration(Exception):
    """A new Manager tried to join the Engine with a BadRegistration message"""

    def __init__(self, worker_id, critical=False):
        self.worker_id = worker_id
        self.tstamp = time.time()
        self.handled = "critical" if critical else "suppressed"

    def __repr__(self):
        return (
            f"Manager {self.worker_id} attempted to register with a bad "
            f"registration message. Caused a {self.handled} failure"
        )

    def __str__(self):
        return self.__repr__()


class Interchange:
    """Interchange is a task orchestrator for distributed systems.

    1. Asynchronously queue large volume of tasks (>100K)
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    4. Service single and batch requests from workers
    5. Be aware of requests worker resource capacity,
       eg. schedule only jobs that fit into walltime.

    TODO: We most likely need a PUB channel to send out global commands, like shutdown
    """

    def __init__(
        self,
        strategy=None,
        poll_period=None,
        heartbeat_period=None,
        heartbeat_threshold=1,
        working_dir=None,
        provider=None,
        max_workers_per_node=None,
        mem_per_worker=None,
        available_accelerators: t.Sequence[str] = (),
        prefetch_capacity=None,
        scheduler_mode=None,
        container_type=None,
        container_cmd_options="",
        worker_mode=None,
        cold_routing_interval=10.0,
        scaling_enabled=True,
        client_address="127.0.0.1",
        interchange_address="127.0.0.1",
        client_ports: tuple[int, int, int] = (50055, 50056, 50057),
        worker_ports=None,
        worker_port_range=None,
        cores_per_worker=1.0,
        worker_debug=False,
        launch_cmd=None,
        logdir=".",
        endpoint_id=None,
        suppress_failure=False,
    ):
        """
        Parameters
        ----------
        config : globus_compute_sdk.Config object
             Globus Compute config object that describes how compute should
             be provisioned

        client_address : str
             The ip address at which the parsl client can be reached.
             Default: "127.0.0.1"

        interchange_address : str
             The ip address at which the workers will be able to reach the Interchange.
             Default: "127.0.0.1"

        client_ports : tuple[int, int, int]
             The ports at which the client can be reached

        launch_cmd : str
             TODO : update

        worker_ports : tuple(int, int)
             The specific two ports at which workers will connect to the Interchange.
             Default: None

        worker_port_range : tuple(int, int)
             The interchange picks ports at random from the range which will be used by
             workers. This is overridden when the worker_ports option is set.
             Default: (54000, 55000)

        cores_per_worker : float
             cores to be assigned to each worker. Oversubscription is possible
             by setting cores_per_worker < 1.0. Default=1

        available_accelerators: sequence of str
            List of device IDs for accelerators available on each node
            Default: Empty list

        container_cmd_options: str
            Container command strings to be added to associated container command.
            For example, singularity exec {container_cmd_options}

        cold_routing_interval: float
            The time interval between warm and cold function routing in SOFT
            scheduler_mode.
            It is ONLY used when using soft scheduler_mode.
            We need this to avoid container workers being idle for too long.
            But we dont't want this cold routing to occur too often,
            since this may cause many warm container workers to switch to a new one.
            Default: 10.0 seconds

        worker_debug : Bool
             Enables worker debug logging.

        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'

        endpoint_id : str
             Identity string that identifies the endpoint to the broker

        suppress_failure : Bool
             When set to True, the interchange will attempt to suppress failures.
             Default: False
        """

        self.logdir = logdir
        os.makedirs(self.logdir, exist_ok=True)
        log.info(f"Initializing Interchange process with Endpoint ID: {endpoint_id}")

        #
        self.max_workers_per_node = max_workers_per_node
        self.mem_per_worker = mem_per_worker
        self.cores_per_worker = cores_per_worker
        self.available_accelerators = available_accelerators
        self.prefetch_capacity = prefetch_capacity

        self.scheduler_mode = scheduler_mode
        self.container_type = container_type
        self.container_cmd_options = container_cmd_options
        self.worker_mode = worker_mode
        self.cold_routing_interval = cold_routing_interval

        self.working_dir = working_dir
        self.provider = provider
        self.worker_debug = worker_debug
        self.scaling_enabled = scaling_enabled

        self.strategy = strategy
        self.client_address = client_address
        self.interchange_address = interchange_address
        self.suppress_failure = suppress_failure

        self.poll_period = poll_period
        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        # initialize the last heartbeat time to start the loop
        self.last_heartbeat = time.time()

        self.serializer = ComputeSerializer()
        log.info(
            "Attempting connection to forwarder at {} on ports: {},{},{}".format(
                client_address, client_ports[0], client_ports[1], client_ports[2]
            )
        )
        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.set_hwm(0)
        self.task_incoming.RCVTIMEO = 10  # in milliseconds
        log.info(f"Task incoming on tcp://{client_address}:{client_ports[0]}")
        self.task_incoming.connect(f"tcp://{client_address}:{client_ports[0]}")

        self.results_outgoing = self.context.socket(zmq.DEALER)
        self.results_outgoing.set_hwm(0)
        log.info(f"Results outgoing on tcp://{client_address}:{client_ports[1]}")
        self.results_outgoing.connect(f"tcp://{client_address}:{client_ports[1]}")

        self.command_channel = self.context.socket(zmq.DEALER)
        self.command_channel.RCVTIMEO = 1000  # in milliseconds
        # self.command_channel.set_hwm(0)
        log.info(f"Command _channel on tcp://{client_address}:{client_ports[2]}")
        self.command_channel.connect(f"tcp://{client_address}:{client_ports[2]}")
        log.info("Connected to forwarder")

        self.pending_task_queue: dict[str, queue.Queue] = {}
        self.containers: dict[str, str] = {}
        self.total_pending_task_count = 0

        log.info(f"Interchange address is {self.interchange_address}")
        self.worker_ports = worker_ports
        self.worker_port_range = (
            worker_port_range if worker_port_range is not None else (54000, 55000)
        )

        self.task_outgoing = self.context.socket(zmq.ROUTER)
        self.task_outgoing.set_hwm(0)
        self.results_incoming = self.context.socket(zmq.ROUTER)
        self.results_incoming.set_hwm(0)

        self.endpoint_id = endpoint_id
        worker_bind_address = f"tcp://{self.interchange_address}"
        log.info(f"Interchange binding worker ports to {worker_bind_address}")
        if self.worker_ports:
            self.worker_task_port = self.worker_ports[0]
            self.worker_result_port = self.worker_ports[1]

            self.task_outgoing.bind(f"{worker_bind_address}:{self.worker_task_port}")
            self.results_incoming.bind(
                f"{worker_bind_address}:{self.worker_result_port}"
            )

        else:
            self.worker_task_port = self.task_outgoing.bind_to_random_port(
                worker_bind_address,
                min_port=worker_port_range[0],
                max_port=worker_port_range[1],
                max_tries=100,
            )
            self.worker_result_port = self.results_incoming.bind_to_random_port(
                worker_bind_address,
                min_port=worker_port_range[0],
                max_port=worker_port_range[1],
                max_tries=100,
            )

        log.info(
            "Bound to ports {},{} for incoming worker connections".format(
                self.worker_task_port, self.worker_result_port
            )
        )

        self._ready_manager_queue: dict[bytes, t.Any] = {}

        self.blocks: dict[str, str] = {}
        self.block_id_map: dict[str, str] = {}
        self.launch_cmd = launch_cmd
        self.last_core_hr_counter = 0
        if not launch_cmd:
            self.launch_cmd = (
                "globus-compute-manager {debug} {max_workers} "
                "-c {cores_per_worker} "
                "--poll {poll_period} "
                "--task_url={task_url} "
                "--result_url={result_url} "
                "--logdir={logdir} "
                "--block_id={{block_id}} "
                "--hb_period={heartbeat_period} "
                "--hb_threshold={heartbeat_threshold} "
                "--worker_mode={worker_mode} "
                "--container_cmd_options='{container_cmd_options}' "
                "--scheduler_mode={scheduler_mode} "
                "--worker_type={{worker_type}} "
                "--available-accelerators {accelerator_list}"
            )

        self.current_platform = {
            "parsl_v": PARSL_VERSION,
            "python_v": "{}.{}.{}".format(
                sys.version_info.major, sys.version_info.minor, sys.version_info.micro
            ),
            "os": platform.system(),
            "hname": platform.node(),
            "dir": os.getcwd(),
        }

        log.info(f"Platform info: {self.current_platform}")
        self._block_counter = 0
        try:
            self.load_config()
        except Exception:
            log.exception("Caught exception")
            raise

        self.task_cancel_running_queue: queue.Queue = queue.Queue()
        self.task_cancel_pending_trap: dict[str, str] = {}
        self.task_status_deltas: dict[str, list[TaskTransition]] = defaultdict(list)
        self._task_status_delta_lock = threading.Lock()
        self.container_switch_count: dict[bytes, int] = {}

    def load_config(self):
        """Load the config"""
        log.info("Loading endpoint local config")
        working_dir = self.working_dir
        if self.working_dir is None:
            working_dir = os.path.join(self.logdir, "worker_logs")
        log.info(f"Setting working_dir: {working_dir}")

        self.provider.script_dir = working_dir
        if hasattr(self.provider, "channel"):
            self.provider.channel.script_dir = os.path.join(
                working_dir, "submit_scripts"
            )
            self.provider.channel.makedirs(
                self.provider.channel.script_dir, exist_ok=True
            )
            os.makedirs(self.provider.script_dir, exist_ok=True)

        debug_opts = "--debug" if self.worker_debug else ""
        max_workers = (
            ""
            if self.max_workers_per_node == float("inf")
            else f"--max_workers={self.max_workers_per_node}"
        )

        worker_task_url = f"tcp://{self.interchange_address}:{self.worker_task_port}"
        worker_result_url = (
            f"tcp://{self.interchange_address}:{self.worker_result_port}"
        )

        l_cmd = self.launch_cmd.format(
            debug=debug_opts,
            max_workers=max_workers,
            cores_per_worker=self.cores_per_worker,
            accelerator_list=" ".join(self.available_accelerators),
            # mem_per_worker=self.mem_per_worker,
            prefetch_capacity=self.prefetch_capacity,
            task_url=worker_task_url,
            result_url=worker_result_url,
            nodes_per_block=self.provider.nodes_per_block,
            heartbeat_period=self.heartbeat_period,
            heartbeat_threshold=self.heartbeat_threshold,
            poll_period=self.poll_period,
            worker_mode=self.worker_mode,
            container_cmd_options=self.container_cmd_options,
            scheduler_mode=self.scheduler_mode,
            logdir=working_dir,
        )

        self.launch_cmd = l_cmd
        log.info(f"Launch command: {self.launch_cmd}")

        if self.scaling_enabled:
            log.info("Scaling ...")
            self.scale_out(self.provider.init_blocks)

    def migrate_tasks_to_internal(self, kill_event):
        """Pull tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        log.info("Starting")
        task_counter = 0
        poller = zmq.Poller()
        poller.register(self.task_incoming, zmq.POLLIN)

        while not kill_event.is_set():
            # We are no longer doing heartbeats on the task side.
            try:
                raw_msg = self.task_incoming.recv()
                self.last_heartbeat = time.time()
            except zmq.Again:
                log.trace(
                    "No new incoming task - %s tasks in internal queue",
                    self.total_pending_task_count,
                )
                continue

            try:
                msg = Message.unpack(raw_msg)
            except Exception:
                log.exception(f"Failed to unpack message, RAW:{raw_msg}")
                continue

            if msg == "STOP":
                # TODO: Yadu. This should be replaced by a proper MessageType
                log.debug("Received STOP message.")
                kill_event.set()
                break
            elif isinstance(msg, Heartbeat):
                log.debug("Got heartbeat")
            else:
                log.info(f"Received task: {msg.task_id}")
                local_container = msg.container_id
                self.containers[local_container] = local_container
                msg.set_local_container(local_container)
                if local_container not in self.pending_task_queue:
                    self.pending_task_queue[local_container] = queue.Queue(
                        maxsize=10**6
                    )

                # We pass the raw message along
                self.pending_task_queue[local_container].put(
                    {
                        "task_id": msg.task_id,
                        "container_id": msg.container_id,
                        "local_container": local_container,
                        "raw_buffer": raw_msg,
                    }
                )
                self.total_pending_task_count += 1
                tt = TaskTransition(
                    timestamp=time.time_ns(),
                    state=TaskState.WAITING_FOR_NODES,
                    actor=ActorName.INTERCHANGE,
                )

                with self._task_status_delta_lock:
                    self.task_status_deltas[msg.task_id].append(tt)

                log.debug(
                    f"[TASK_PULL_THREAD] task {msg.task_id} is now WAITING_FOR_NODES"
                )
                log.debug(
                    "[TASK_PULL_THREAD] pending task count: {}".format(
                        self.total_pending_task_count
                    )
                )
                task_counter += 1
                log.debug(f"[TASK_PULL_THREAD] Fetched task:{task_counter}")

    def get_total_tasks_outstanding(self):
        """Get the outstanding tasks in total"""
        outstanding = {}
        for task_type in self.pending_task_queue:
            outstanding[task_type] = (
                outstanding.get(task_type, 0)
                + self.pending_task_queue[task_type].qsize()
            )
        for manager in self._ready_manager_queue:
            for task_type in self._ready_manager_queue[manager]["tasks"]:
                outstanding[task_type] = outstanding.get(task_type, 0) + len(
                    self._ready_manager_queue[manager]["tasks"][task_type]
                )
        return outstanding

    def get_total_live_workers(self):
        """Get the total active workers"""
        active = 0
        for manager in self._ready_manager_queue:
            if self._ready_manager_queue[manager]["active"]:
                active += self._ready_manager_queue[manager]["max_worker_count"]
        return active

    def get_outstanding_breakdown(self):
        """Get outstanding breakdown per manager and in the interchange queues

        Returns
        -------
        List of status for online elements
        [ (element, tasks_pending, status) ... ]
        """

        pending_on_interchange = self.total_pending_task_count
        # Reporting pending on interchange is a deviation from Parsl
        reply = [("interchange", pending_on_interchange, True)]
        for manager in self._ready_manager_queue:
            resp = (
                manager.decode("utf-8"),
                sum(
                    len(tids)
                    for tids in self._ready_manager_queue[manager]["tasks"].values()
                ),
                self._ready_manager_queue[manager]["active"],
            )
            reply.append(resp)
        return reply

    def _hold_block(self, block_id):
        """Sends hold command to all managers which are in a specific block

        Parameters
        ----------
        block_id : str
             Block identifier of the block to be put on hold
        """
        for manager in self._ready_manager_queue:
            if (
                self._ready_manager_queue[manager]["active"]
                and self._ready_manager_queue[manager]["block_id"] == block_id
            ):
                log.debug(f"[HOLD_BLOCK]: Sending hold to manager: {manager}")
                self.hold_manager(manager)

    def hold_manager(self, manager):
        """Put manager on hold
        Parameters
        ----------

        manager : str
          Manager id to be put on hold while being killed
        """
        if manager in self._ready_manager_queue:
            self._ready_manager_queue[manager]["active"] = False

    def _status_report_loop(self, kill_event, status_report_queue: queue.Queue):
        log.info(f"Endpoint id: {self.endpoint_id}")

        def _enqueue_status_report(ep_state: dict, task_states: dict):
            try:
                msg = EPStatusReport(str(self.endpoint_id), ep_state, dict(task_states))
                status_report_queue.put(msg.pack())
            except Exception:
                log.exception("Unable to create or send EP status report.")
                log.debug("Attempted to send chunk: %s", tsd_chunk)
                # ignoring so that the thread continues; "it's just a status"

        while True:
            with self._task_status_delta_lock:
                task_status_deltas = copy.deepcopy(self.task_status_deltas)
                self.task_status_deltas.clear()

            log.debug(
                "Cleared task deltas (%s); sending status report to Engine.",
                len(task_status_deltas),
            )

            # For multi-chunk reports ("lots-o-tasks"), the state won't change *that*
            # much each iteration, so cache the result
            global_state = self.get_global_state_for_status_report()

            # The result processor will gracefully handle any size message, but
            # courtesy says to chunk work; 4,096 is empirically chosen to be plenty
            # "bulk enough," but not rude.
            for tsd_chunk in chunk_by(task_status_deltas.items(), 4_096):
                _enqueue_status_report(global_state, dict(tsd_chunk))

            if not task_status_deltas:
                _enqueue_status_report(global_state, {})

            del task_status_deltas  # free some memory in "large case"

            if kill_event.wait(self.heartbeat_period):
                break

    def _command_server(self, kill_event):
        """Command server to run async command to the interchange

        We want to be able to receive the following not yet implemented/updated
        commands:
         - OutstandingCount
         - ListManagers (get outstanding broken down by manager)
         - HoldWorker
         - Shutdown
        """
        log.debug("Command Server Starting")

        while not kill_event.is_set():
            try:
                buffer = self.command_channel.recv()
                log.debug(f"Received command request {buffer}")
                command = Message.unpack(buffer)

                if command.type is MessageType.TASK_CANCEL:
                    log.info(f"Received TASK_CANCEL for Task:{command.task_id}")
                    self.enqueue_task_cancel(command.task_id)
                    reply = command

                elif command.type is MessageType.HEARTBEAT_REQ:
                    log.info("Received synchonous HEARTBEAT_REQ from hub")
                    log.info(f"Replying with Heartbeat({self.endpoint_id})")
                    reply = Heartbeat(self.endpoint_id)

                else:
                    log.error(
                        f"Received unsupported message type:{command.type} on "
                        "command _channel"
                    )
                    reply = BadCommand(f"Unknown command type: {command.type}")

                log.debug(f"Reply: {reply}")
                self.command_channel.send(reply.pack())

            except zmq.Again:
                log.trace("Command server is alive")
                continue

    def enqueue_task_cancel(self, task_id):
        """Cancel a task on the interchange
        Here are the task states and responses we issue here
        1. Task is pending in queues -> we add task to a trap to capture while in
            dispatch and delegate cancel to the manager the task is assigned to
        2. Task is in a transitionary state between pending in queue and dispatched ->
               task is added pre-emptively to trap
        3. Task is pending on a manager -> we delegate cancellation to manager
        4. Task is already complete -> we leave in trap, since we can't know

        We place the task in the trap so that even if the search misses, the task
        will be caught from getting dispatched even if the search fails due to a
        race-condition. Since the task can't be dispatched before scheduling is
        complete, either must work.
        """
        log.debug(f"Received task_cancel request for Task:{task_id}")

        self.task_cancel_pending_trap[task_id] = task_id
        for manager in self._ready_manager_queue:
            for task_type in self._ready_manager_queue[manager]["tasks"]:
                for tid in self._ready_manager_queue[manager]["tasks"][task_type]:
                    if tid == task_id:
                        log.debug(
                            f"Task:{task_id} is running, "
                            "moving task_cancel message onto queue"
                        )
                        self.task_cancel_running_queue.put((manager, task_id))
                        self.task_cancel_pending_trap.pop(task_id, None)
                        break
        return

    def handle_sigterm(self, sig_num, curr_stack_frame):
        log.warning("Received SIGTERM, stopping")
        self.stop()

    def stop(self):
        """Prepare the interchange for shutdown"""
        self._kill_event.set()

        self._task_puller_thread.join()
        self._command_thread.join()
        self._status_report_thread.join()
        log.debug("HighThroughput Interchange stopped")

    def start(self, poll_period: int | None = None) -> None:
        """Start the Interchange

        Parameters:
        ----------
        poll_period : int
           poll_period in milliseconds
        """
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        log.info("Incoming ports bound")

        if poll_period is None:
            poll_period = self.poll_period

        start = time.time()
        count = 0

        self._kill_event = threading.Event()
        self._task_puller_thread = threading.Thread(
            target=self.migrate_tasks_to_internal,
            args=(self._kill_event,),
            name="TASK_PULL_THREAD",
        )
        self._task_puller_thread.start()

        self._command_thread = threading.Thread(
            target=self._command_server, args=(self._kill_event,), name="COMMAND_THREAD"
        )
        self._command_thread.start()

        status_report_queue: queue.Queue[bytes] = queue.Queue()
        self._status_report_thread = threading.Thread(
            target=self._status_report_loop,
            args=(self._kill_event, status_report_queue),
            name="STATUS_THREAD",
        )
        self._status_report_thread.start()

        try:
            log.info("Starting strategy.")
            self.strategy.start(self)
        except RuntimeError:
            # This is raised when re-registering an endpoint as strategy already exists
            log.exception("Failed to start strategy.")

        poller = zmq.Poller()
        # poller.register(self.task_incoming, zmq.POLLIN)
        poller.register(self.task_outgoing, zmq.POLLIN)
        poller.register(self.results_incoming, zmq.POLLIN)

        # These are managers which we should examine in an iteration
        # for scheduling a job (or maybe any other attention?).
        # Anything altering the state of the manager should add it
        # onto this list.
        interesting_managers: set[bytes] = set()

        # This value records when the last cold routing in soft mode happens
        # When the cold routing in soft mode happens, it may cause worker containers to
        # switch
        # Cold routing is to reduce the number idle workers of specific task types on
        # the managers when there are not enough tasks of those types in the task queues
        # on interchange
        last_cold_routing_time = time.time()
        prev_manager_stat = None

        task_deltas_to_merge: dict[str, list[TaskTransition]] = defaultdict(list)

        while not self._kill_event.is_set():
            self.socks = dict(poller.poll(timeout=poll_period))

            # Listen for requests for work
            if (
                self.task_outgoing in self.socks
                and self.socks[self.task_outgoing] == zmq.POLLIN
            ):
                log.trace("starting task_outgoing section")
                message = self.task_outgoing.recv_multipart()
                manager = message[0]

                mdata = self._ready_manager_queue.get(manager)
                if not mdata:
                    reg_flag = False

                    try:
                        msg = json.loads(message[1].decode("utf-8"))
                        reg_flag = True
                    except Exception:
                        log.warning(
                            "Got a non-json registration message from manager:%s",
                            manager,
                        )
                        log.debug("Message :\n%s\n", message)

                    # By default we set up to ignore bad nodes/registration messages.
                    now = time.time()
                    mdata = {
                        "last": now,
                        "reg_time": now,
                        "free_capacity": {"total_workers": 0},
                        "max_worker_count": 0,
                        "active": True,
                        "tasks": collections.defaultdict(set),
                        "total_tasks": 0,
                    }
                    if reg_flag is True:
                        interesting_managers.add(manager)
                        log.info(
                            f"Add manager to ready queue: {manager!r}"
                            f"\n  Registration info: {msg})"
                        )
                        mdata.update(msg)
                        self._ready_manager_queue[manager] = mdata

                        if (
                            msg["python_v"].rsplit(".", 1)[0]
                            != self.current_platform["python_v"].rsplit(".", 1)[0]
                            or msg["parsl_v"] != self.current_platform["parsl_v"]
                        ):
                            log.info(
                                f"Manager:{manager!r} version:{msg['python_v']} "
                                "does not match the interchange"
                            )
                    else:
                        # Registration has failed.
                        if self.suppress_failure is False:
                            log.debug("Setting kill event for bad manager")
                            self._kill_event.set()
                            e = BadRegistration(manager, critical=True)
                            result_package = {
                                "task_id": -1,
                                "exception": self.serializer.serialize(e),
                            }
                            pkl_package = dill.dumps(result_package)
                            self.results_outgoing.send(dill.dumps([pkl_package]))
                        else:
                            log.debug(
                                "Suppressing bad registration from manager: %s",
                                manager,
                            )

                else:
                    mdata["last"] = time.time()
                    if message[1] == b"HEARTBEAT":
                        log.debug("Manager %s sends heartbeat", manager)
                        self.task_outgoing.send_multipart(
                            [manager, b"", PKL_HEARTBEAT_CODE]
                        )
                    else:
                        manager_adv = dill.loads(message[1])
                        log.debug("Manager %s requested %s", manager, manager_adv)
                        manager_adv["total_workers"] = sum(manager_adv["free"].values())
                        mdata["free_capacity"].update(manager_adv)
                        interesting_managers.add(manager)
                        del manager_adv

            # If we had received any requests, check if there are tasks that could be
            # passed

            cur_manager_stat = len(self._ready_manager_queue), len(interesting_managers)
            if cur_manager_stat != prev_manager_stat:
                prev_manager_stat = cur_manager_stat
                _msg = "[MAIN] New managers count (total/interesting): {}/{}"
                log.debug(_msg.format(*cur_manager_stat))

            if time.time() - last_cold_routing_time > self.cold_routing_interval:
                task_dispatch, dispatched_task = naive_interchange_task_dispatch(
                    interesting_managers,
                    self.pending_task_queue,
                    self._ready_manager_queue,
                    scheduler_mode=self.scheduler_mode,
                    cold_routing=True,
                )
                last_cold_routing_time = time.time()
            else:
                task_dispatch, dispatched_task = naive_interchange_task_dispatch(
                    interesting_managers,
                    self.pending_task_queue,
                    self._ready_manager_queue,
                    scheduler_mode=self.scheduler_mode,
                    cold_routing=False,
                )

            self.total_pending_task_count -= dispatched_task

            # Task cancel is high priority, so we'll process all requests
            # in one go
            try:
                while True:
                    manager, task_id = self.task_cancel_running_queue.get(block=False)
                    log.debug(
                        "CANCELLED running task (id: %s, manager: %s)", task_id, manager
                    )
                    cancel_message = dill.dumps(("TASK_CANCEL", task_id))
                    self.task_outgoing.send_multipart([manager, b"", cancel_message])
            except queue.Empty:
                pass

            for manager in task_dispatch:
                tasks = task_dispatch[manager]
                if tasks:
                    log.info(
                        'Sending task message "{}..." to manager {!r}'.format(
                            str(tasks)[:50], manager
                        )
                    )
                    serializd_raw_tasks_buffer = dill.dumps(tasks)
                    self.task_outgoing.send_multipart(
                        [manager, b"", serializd_raw_tasks_buffer]
                    )

                    for task in tasks:
                        task_id = task["task_id"]
                        log.info(f"Sent task {task_id} to manager {manager!r}")
                        if (
                            self.task_cancel_pending_trap
                            and task_id in self.task_cancel_pending_trap
                        ):
                            log.info(f"Task:{task_id} CANCELLED before launch")
                            cancel_message = dill.dumps(("TASK_CANCEL", task_id))
                            self.task_outgoing.send_multipart(
                                [manager, b"", cancel_message]
                            )
                            self.task_cancel_pending_trap.pop(task_id)
                        else:
                            log.debug("Task:%s is now WAITING_FOR_LAUNCH", task_id)
                            tt = TaskTransition(
                                timestamp=time.time_ns(),
                                state=TaskState.WAITING_FOR_LAUNCH,
                                actor=ActorName.INTERCHANGE,
                            )
                            task_deltas_to_merge[task_id].append(tt)

            if task_deltas_to_merge:
                with self._task_status_delta_lock:
                    for task_id, deltas in task_deltas_to_merge.items():
                        self.task_status_deltas[task_id].extend(deltas)
                task_deltas_to_merge.clear()

            # Receive any results and forward to client
            if (
                self.results_incoming in self.socks
                and self.socks[self.results_incoming] == zmq.POLLIN
            ):
                log.debug("entering results_incoming section")
                manager, *b_messages = self.results_incoming.recv_multipart()
                mdata = self._ready_manager_queue.get(manager)
                if not mdata:
                    log.warning(
                        "Received a result from a un-registered manager: %s",
                        manager,
                    )
                else:
                    # We expect the batch of messages to be (optionally) a task status
                    # update message followed by 0 or more task results
                    try:
                        log.debug("Trying to unpack")
                        manager_report = Message.unpack(b_messages[0])
                        if manager_report.task_statuses:
                            log.info(
                                "Got manager status report: %s",
                                manager_report.task_statuses,
                            )

                            for tid, statuses in manager_report.task_statuses.items():
                                task_deltas_to_merge[tid].extend(statuses)

                        self.task_outgoing.send_multipart(
                            [manager, b"", PKL_HEARTBEAT_CODE]
                        )
                        b_messages = b_messages[1:]
                        mdata["last"] = time.time()
                        self.container_switch_count[manager] = (
                            manager_report.container_switch_count
                        )
                        log.info(
                            "Got container switch count: %s",
                            self.container_switch_count,
                        )
                    except Exception:
                        pass
                    if len(b_messages):
                        log.info(f"Got {len(b_messages)} result items in batch")
                        for idx, b_message in enumerate(b_messages):
                            r = dill.loads(b_message)
                            tid = r["task_id"]

                            log.debug("Received task result %s (from %s)", tid, manager)
                            task_container = self.containers[r["container_id"]]
                            log.debug(
                                "Removing for manager: %s from %s",
                                manager,
                                self._ready_manager_queue,
                            )

                            mdata["tasks"][task_container].remove(tid)
                            b_messages[idx] = dill.dumps(r)

                        mdata["total_tasks"] -= len(b_messages)

                    self.results_outgoing.send(dill.dumps(b_messages))
                    interesting_managers.add(manager)

                    log.debug(f"Current tasks: {mdata['tasks']}")
                log.debug("leaving results_incoming section")

            # Send status reports from this main thread to avoid thread-safety on zmq
            # sockets
            try:
                packed_status_report = status_report_queue.get(block=False)
                log.trace("forwarding status report: %s", packed_status_report)
                self.results_outgoing.send(packed_status_report)
            except queue.Empty:
                pass

            now = time.time()
            hbt_window_start = now - self.heartbeat_threshold
            bad_managers = [
                manager
                for manager, mdata in self._ready_manager_queue.items()
                if hbt_window_start > mdata["last"]
            ]
            bad_manager_msgs = []
            for manager in bad_managers:
                log.debug(
                    "Last: %s Current: %s",
                    self._ready_manager_queue[manager]["last"],
                    now,
                )
                log.warning(f"Too many heartbeats missed for manager {manager!r}")
                for tasks in self._ready_manager_queue[manager]["tasks"].values():
                    for tid in tasks:
                        try:
                            raise ManagerLost(manager)
                        except Exception:
                            result_package = {
                                "task_id": tid,
                                "exception": get_error_string(),
                                "error_details": get_result_error_details(),
                            }
                            pkl_package = dill.dumps(result_package)
                            bad_manager_msgs.append(pkl_package)
                log.warning(f"Unregistering manager {manager!r}")
                self._ready_manager_queue.pop(manager, None)
                if manager in interesting_managers:
                    interesting_managers.remove(manager)
            if bad_manager_msgs:
                log.warning(f"Sending task failure reports of manager {manager!r}")
                self.results_outgoing.send(dill.dumps(bad_manager_msgs))

        delta = time.time() - start
        log.info(f"Processed {count} tasks in {delta} seconds")
        log.warning("Exiting")

    def get_global_state_for_status_report(self):
        outstanding_tasks = self.get_total_tasks_outstanding()
        pending_tasks = self.total_pending_task_count
        num_managers = len(self._ready_manager_queue)
        live_workers = self.get_total_live_workers()
        free_capacity = sum(
            m["free_capacity"]["total_workers"]
            for m in self._ready_manager_queue.values()
        )

        return {
            "managers": num_managers,
            "total_workers": live_workers,
            "idle_workers": free_capacity,
            "pending_tasks": pending_tasks,
            "outstanding_tasks": outstanding_tasks,
            "heartbeat_period": self.heartbeat_period,
        }

    def scale_out(self, blocks=1, task_type=None):
        """Scales out the number of blocks by "blocks"

        Raises:
             NotImplementedError
        """
        log.info(f"Scaling out by {blocks} more blocks for task type {task_type}")
        r = []
        for _i in range(blocks):
            if self.provider:
                self._block_counter += 1
                external_block_id = str(self._block_counter)
                if not task_type and self.scheduler_mode == "hard":
                    launch_cmd = self.launch_cmd.format(
                        block_id=external_block_id, worker_type="RAW"
                    )
                else:
                    launch_cmd = self.launch_cmd.format(
                        block_id=external_block_id, worker_type=task_type
                    )
                if not task_type:
                    internal_block = self.provider.submit(launch_cmd, 1)
                else:
                    internal_block = self.provider.submit(launch_cmd, 1, task_type)
                log.debug(f"Launched block {external_block_id}->{internal_block}")
                if not internal_block:
                    raise RuntimeError(
                        "Attempt to provision nodes via provider "
                        f"{self.provider.label} has failed"
                    )
                self.blocks[external_block_id] = internal_block
                self.block_id_map[internal_block] = external_block_id
            else:
                log.error("No execution provider available")
                r = None
        return r

    def scale_in(self, blocks=None, block_ids=None, task_type=None):
        """Scale in the number of active blocks by specified amount.

        Parameters
        ----------
        blocks : int
            # of blocks to terminate

        block_ids : [str.. ]
            List of external block ids to terminate
        """
        if block_ids is None:
            block_ids = []
        if task_type:
            log.info(
                "Scaling in blocks of specific task type %s. Let the provider decide "
                "which to kill",
                task_type,
            )
            if self.scaling_enabled and self.provider:
                to_kill, r = self.provider.cancel(blocks, task_type)
                log.info(f"Get the killed blocks: {to_kill}, and status: {r}")
                for job in to_kill:
                    log.info(
                        "[scale_in] Getting the block_id map {} for job {}".format(
                            self.block_id_map, job
                        )
                    )
                    block_id = self.block_id_map[job]
                    log.info(f"[scale_in] Holding block {block_id}")
                    self._hold_block(block_id)
                    self.blocks.pop(block_id)
                return r

        if block_ids:
            block_ids_to_kill = block_ids
        else:
            block_ids_to_kill = list(self.blocks.keys())[:blocks]

        # Try a polite terminate
        # TODO : Missing logic to hold blocks
        for block_id in block_ids_to_kill:
            self._hold_block(block_id)

        # Now kill via provider
        to_kill = [self.blocks.pop(bid) for bid in block_ids_to_kill]

        if self.scaling_enabled and self.provider:
            r = self.provider.cancel(to_kill)

        return r

    def provider_status(self):
        """Get status of all blocks from the provider. The return type is
        defined by the particular provider in use.
        """
        status = []
        if self.provider:
            job_ids: list[str] = list(self.blocks.values())
            log.trace("Getting the status of %s blocks.", job_ids)
            status = self.provider.status(job_ids)
            log.trace("The status is %s", status)

        return status


def starter(comm_q: mp.Queue, *args, **kwargs) -> None:
    """Start the interchange process

    The Engine is expected to call this function. The args, kwargs match that of the
    Interchange.__init__
    """
    ic = None
    try:
        ic = Interchange(*args, **kwargs)
        comm_q.put((ic.worker_task_port, ic.worker_result_port))
    finally:
        if not ic:  # There was an exception
            comm_q.put(None)

        # no sense in having the queue open past it's usefulness
        comm_q.close()
        comm_q.join_thread()
        del comm_q

    if ic:
        ic.start()


def cli_run():
    from globus_compute_endpoint.logging_config import setup_logging

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--client_address", required=True, help="Client address")
    parser.add_argument(
        "--client_ports",
        required=True,
        help="client ports as a triple of outgoing,incoming,command",
    )
    parser.add_argument("--worker_port_range", help="Worker port range as a tuple")
    parser.add_argument(
        "-l",
        "--logdir",
        default="./parsl_worker_logs",
        help="Parsl worker log directory",
    )
    parser.add_argument(
        "-p", "--poll_period", help="REQUIRED: poll period used for main thread"
    )
    parser.add_argument(
        "--worker_ports",
        default=None,
        help="OPTIONAL, pair of workers ports to listen on, "
        "eg --worker_ports=50001,50005",
    )
    parser.add_argument(
        "--suppress_failure",
        action="store_true",
        help="Enables suppression of failures",
    )
    parser.add_argument(
        "--endpoint_id",
        default=None,
        help="Endpoint ID, used to identify the endpoint to the remote broker",
    )
    parser.add_argument("--hb_threshold", help="Heartbeat threshold in seconds")
    parser.add_argument(
        "--config",
        default=None,
        help="Configuration object that describes provisioning",
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Enables debug logging"
    )

    print("Starting HTEX Intechange")

    args = parser.parse_args()

    args.logdir = os.path.abspath(args.logdir)
    if args.worker_ports:
        args.worker_ports = [int(i) for i in args.worker_ports.split(",")]
    if args.worker_port_range:
        args.worker_port_range = [int(i) for i in args.worker_port_range.split(",")]

    setup_logging(
        logfile=os.path.join(args.logdir, "interchange.log"),
        debug=args.debug,
        console_enabled=False,
    )

    with daemon.DaemonContext():
        ic = Interchange(
            logdir=args.logdir,
            suppress_failure=args.suppress_failure,
            client_address=args.client_address,
            client_ports=[int(i) for i in args.client_ports.split(",")],
            endpoint_id=args.endpoint_id,
            config=args.config,
            worker_ports=args.worker_ports,
            worker_port_range=args.worker_port_range,
        )
        ic.start()
