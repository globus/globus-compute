#!/usr/bin/env python
import argparse
import logging
import os
import pickle
import platform
import queue
import signal
import sys
import threading
import time
from queue import Queue
from typing import Tuple

import zmq
from parsl.executors.errors import ScalingFailed
from parsl.version import VERSION as PARSL_VERSION
from retry.api import retry_call

from funcx import __version__ as funcx_sdk_version
from funcx.sdk.client import FuncXClient
from funcx.serialize import FuncXSerializer
from funcx_endpoint import __version__ as funcx_endpoint_version
from funcx_endpoint.endpoint.register_endpoint import register_endpoint
from funcx_endpoint.endpoint.taskqueue import TaskQueue
from funcx_endpoint.executors.high_throughput.mac_safe_queue import mpQueue
from funcx_endpoint.executors.high_throughput.messages import (
    COMMAND_TYPES,
    Heartbeat,
    Message,
    MessageType,
    ResultsAck,
    Task,
    TaskStatusCode,
)
from funcx_endpoint.logging_config import setup_logging

log = logging.getLogger(__name__)

LOOP_SLOWDOWN = 0.0  # in seconds
HEARTBEAT_CODE = (2 ** 32) - 1
PKL_HEARTBEAT_CODE = pickle.dumps(HEARTBEAT_CODE)


class ShutdownRequest(Exception):
    """Exception raised when any async component receives a ShutdownRequest"""

    def __init__(self):
        self.tstamp = time.time()

    def __repr__(self):
        return f"Shutdown request received at {self.tstamp}"


class ManagerLost(Exception):
    """Task lost due to worker loss. Worker is considered lost when multiple heartbeats
    have been missed.
    """

    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.tstamp = time.time()

    def __repr__(self):
        return f"Task failure due to loss of worker {self.worker_id}"


class EndpointInterchange:
    """Interchange is a task orchestrator for distributed systems.

    1. Asynchronously queue large volume of tasks (>100K)
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    4. Service single and batch requests from workers
    5. Be aware of requests worker resource capacity,
       eg. schedule only jobs that fit into walltime.

    TODO: We most likely need a PUB channel to send out global commandzs, like shutdown
    """

    def __init__(
        self,
        config,
        client_address="127.0.0.1",
        interchange_address="127.0.0.1",
        client_ports: Tuple[int, int, int] = (50055, 50056, 50057),
        launch_cmd=None,
        logdir=".",
        endpoint_id=None,
        keys_dir=".curve",
        suppress_failure=True,
        endpoint_dir=".",
        endpoint_name="default",
        reg_info=None,
        funcx_client_options=None,
        results_ack_handler=None,
    ):
        """
        Parameters
        ----------
        config : funcx.Config object
             Funcx config object that describes how compute should be provisioned

        client_address : str
             The ip address at which the parsl client can be reached.
             Default: "127.0.0.1"

        interchange_address : str
             The ip address at which the workers will be able to reach the Interchange.
             Default: "127.0.0.1"

        client_ports : Tuple[int, int, int]
             The ports at which the client can be reached

        launch_cmd : str
             TODO : update

        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'

        keys_dir : str
             Directory from where keys used for communicating with the funcX
             service (forwarders) are stored

        endpoint_id : str
             Identity string that identifies the endpoint to the broker

        suppress_failure : Bool
             When set to True, the interchange will attempt to suppress failures.
             Default: False

        endpoint_dir : str
             Endpoint directory path to store registration info in

        endpoint_name : str
             Name of endpoint

        reg_info : Dict
             Registration info from initial registration on endpoint start, if it
             succeeded

        funcx_client_options : Dict
             FuncXClient initialization options
        """
        self.logdir = logdir
        log.info(
            "Initializing EndpointInterchange process with Endpoint ID: {}".format(
                endpoint_id
            )
        )
        self.config = config
        log.info(f"Got config: {config}")

        self.client_address = client_address
        self.interchange_address = interchange_address
        self.client_ports = client_ports
        self.suppress_failure = suppress_failure

        self.endpoint_dir = endpoint_dir
        self.endpoint_name = endpoint_name

        if funcx_client_options is None:
            funcx_client_options = {}
        self.funcx_client = FuncXClient(**funcx_client_options)

        self.initial_registration_complete = False
        if reg_info:
            self.initial_registration_complete = True
            self.apply_reg_info(reg_info)

        self.heartbeat_period = self.config.heartbeat_period
        self.heartbeat_threshold = self.config.heartbeat_threshold
        # initalize the last heartbeat time to start the loop
        self.last_heartbeat = time.time()
        self.keys_dir = keys_dir
        self.serializer = FuncXSerializer()

        self.pending_task_queue = Queue()
        self.containers = {}
        self.total_pending_task_count = 0

        self._quiesce_event = threading.Event()
        self._kill_event = threading.Event()

        self.results_ack_handler = results_ack_handler

        log.info(f"Interchange address is {self.interchange_address}")

        self.endpoint_id = endpoint_id

        self.current_platform = {
            "parsl_v": PARSL_VERSION,
            "python_v": "{}.{}.{}".format(
                sys.version_info.major, sys.version_info.minor, sys.version_info.micro
            ),
            "libzmq_v": zmq.zmq_version(),
            "pyzmq_v": zmq.pyzmq_version(),
            "os": platform.system(),
            "hname": platform.node(),
            "funcx_sdk_version": funcx_sdk_version,
            "funcx_endpoint_version": funcx_endpoint_version,
            "registration": self.endpoint_id,
            "dir": os.getcwd(),
        }

        log.info(f"Platform info: {self.current_platform}")
        try:
            self.load_config()
        except Exception:
            log.exception("Caught exception")
            raise

        self.tasks = set()
        self.task_status_deltas = {}

        self._test_start = False

    def load_config(self):
        """Load the config"""
        log.info("Loading endpoint local config")

        self.results_passthrough = mpQueue()
        self.executors = {}
        for executor in self.config.executors:
            log.info(f"Initializing executor: {executor.label}")
            executor.funcx_service_address = self.config.funcx_service_address
            if not executor.endpoint_id:
                executor.endpoint_id = self.endpoint_id
            else:
                if not executor.endpoint_id == self.endpoint_id:
                    raise Exception("InconsistentEndpointId")
            self.executors[executor.label] = executor
            if executor.run_dir is None:
                executor.run_dir = self.logdir

    def start_executors(self):
        log.info("Starting Executors")
        for executor in self.config.executors:
            if hasattr(executor, "passthrough") and executor.passthrough is True:
                executor.start(results_passthrough=self.results_passthrough)

    def apply_reg_info(self, reg_info):
        self.client_address = reg_info["public_ip"]
        self.client_ports = (
            reg_info["tasks_port"],
            reg_info["results_port"],
            reg_info["commands_port"],
        )

    def register_endpoint(self):
        reg_info = register_endpoint(
            self.funcx_client, self.endpoint_id, self.endpoint_dir, self.endpoint_name
        )
        self.apply_reg_info(reg_info)
        return reg_info

    def migrate_tasks_to_internal(self, quiesce_event):
        """Pull tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue

        Parameters:
        -----------
        quiesce_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        log.info("[TASK_PULL_THREAD] Starting")

        try:
            self._task_puller_loop(quiesce_event)
        except Exception:
            log.exception("[TASK_PULL_THREAD] Unhandled exception")
        finally:
            quiesce_event.set()
            self.task_incoming.close()
            log.info("[TASK_PULL_THREAD] Thread loop exiting")

    def _task_puller_loop(self, quiesce_event):
        task_counter = 0
        # Create the incoming queue in the thread to keep
        # zmq.context in the same thread. zmq.context is not thread-safe
        self.task_incoming = TaskQueue(
            self.client_address,
            port=self.client_ports[0],
            identity=self.endpoint_id,
            mode="client",
            set_hwm=True,
            keys_dir=self.keys_dir,
            RCVTIMEO=1000,
            linger=0,
        )

        self.task_incoming.put("forwarder", pickle.dumps(self.current_platform))
        log.info(f"Task incoming on tcp://{self.client_address}:{self.client_ports[0]}")

        self.last_heartbeat = time.time()

        while not quiesce_event.is_set():

            try:
                if int(time.time() - self.last_heartbeat) > self.heartbeat_threshold:
                    log.critical(
                        "[TASK_PULL_THREAD] Missed too many heartbeats. "
                        "Setting quiesce event."
                    )
                    quiesce_event.set()
                    break

                try:
                    # TODO : Check the kwarg options for get
                    raw_msg = self.task_incoming.get()[0]
                    self.last_heartbeat = time.time()
                except zmq.Again:
                    # We just timed out while attempting to receive
                    log.debug(
                        "[TASK_PULL_THREAD] {} tasks in internal queue".format(
                            self.total_pending_task_count
                        )
                    )
                    continue
                except Exception:
                    log.exception(
                        "[TASK_PULL_THREAD] Unknown exception while waiting for tasks"
                    )

                # YADU: TODO We need to do the routing here
                try:
                    msg = Message.unpack(raw_msg)
                except Exception:
                    log.exception(
                        "[TASK_PULL_THREAD] Failed to unpack message from forwarder"
                    )
                    pass

                if msg == "STOP":
                    self._kill_event.set()
                    quiesce_event.set()
                    break

                elif isinstance(msg, Heartbeat):
                    log.info("[TASK_PULL_THREAD] Got heartbeat from funcx-forwarder")

                elif isinstance(msg, Task):
                    log.info(f"[TASK_PULL_THREAD] Received task:{msg.task_id}")
                    self.pending_task_queue.put(msg)
                    self.total_pending_task_count += 1
                    self.task_status_deltas[
                        msg.task_id
                    ] = TaskStatusCode.WAITING_FOR_NODES
                    task_counter += 1
                    log.debug(
                        "[TASK_PULL_THREAD] Task counter:%s Pending Tasks: %s",
                        task_counter,
                        self.total_pending_task_count,
                    )

                elif isinstance(msg, ResultsAck):
                    self.results_ack_handler.ack(msg.task_id)

                else:
                    log.warning(
                        f"[TASK_PULL_THREAD] Unknown message type received: {msg}"
                    )

            except Exception:
                log.exception("[TASK_PULL_THREAD] Something really bad happened")
                continue

    def get_container(self, container_uuid):
        """Get the container image location if it is not known to the interchange"""
        if container_uuid not in self.containers:
            if container_uuid == "RAW" or not container_uuid:
                self.containers[container_uuid] = "RAW"
            else:
                try:
                    container = self.funcx_client.get_container(
                        container_uuid, self.config.container_type
                    )
                except Exception:
                    log.exception(
                        "[FETCH_CONTAINER] Unable to resolve container location"
                    )
                    self.containers[container_uuid] = "RAW"
                else:
                    log.info(f"[FETCH_CONTAINER] Got container info: {container}")
                    self.containers[container_uuid] = container.get("location", "RAW")
        return self.containers[container_uuid]

    def _command_server(self, quiesce_event):
        """Command server to run async command to the interchange

        We want to be able to receive the following not yet implemented/updated
        commands:
         - OutstandingCount
         - ListManagers (get outstanding broken down by manager)
         - HoldWorker
         - Shutdown
        """
        log.debug("[COMMAND] Command Server Starting")

        try:
            self._command_server_loop(quiesce_event)
        except Exception:
            log.exception("[COMMAND] Unhandled exception")
        finally:
            quiesce_event.set()
            self.command_channel.close()
            log.info("[COMMAND] Thread loop exiting")

    def _command_server_loop(self, quiesce_event):
        self.command_channel = TaskQueue(
            self.client_address,
            port=self.client_ports[2],
            identity=self.endpoint_id,
            mode="client",
            RCVTIMEO=1000,  # in milliseconds
            keys_dir=self.keys_dir,
            set_hwm=True,
            linger=0,
        )

        # TODO :Register all channels with the authentication string.
        self.command_channel.put(
            "forwarder", pickle.dumps({"registration": self.endpoint_id})
        )

        while not quiesce_event.is_set():
            try:
                # Wait for 1000 ms
                buffer = self.command_channel.get(timeout=1000)
                log.debug(f"[COMMAND] Received command request {buffer}")
                command = Message.unpack(buffer)
                if command.type not in COMMAND_TYPES:
                    log.error("Received incorrect message type on command channel")
                    self.command_channel.put(bytes())
                    continue

                if command.type is MessageType.HEARTBEAT_REQ:
                    log.info("[COMMAND] Received synchonous HEARTBEAT_REQ from hub")
                    log.info(f"[COMMAND] Replying with Heartbeat({self.endpoint_id})")
                    reply = Heartbeat(self.endpoint_id)

                log.debug(f"[COMMAND] Reply: {reply}")
                self.command_channel.put(reply.pack())

            except zmq.Again:
                # log.debug("[COMMAND] is alive")
                continue

    def quiesce(self):
        """Temporarily stop everything on the interchange in order to reach a consistent
        state before attempting to start again. This must be called on the main thread
        """
        log.info("Interchange Quiesce in progress (stopping and joining all threads)")
        self._quiesce_event.set()
        self._task_puller_thread.join()
        self._command_thread.join()

        log.info("Saving unacked results to disk")
        try:
            self.results_ack_handler.persist()
        except Exception:
            log.exception("Caught exception while saving unacked results")
            log.warning("Interchange will continue without saving unacked results")

        # this must be called last to ensure the next interchange run will occur
        self._quiesce_event.clear()

    def stop(self):
        """Prepare the interchange for shutdown"""
        log.info("Shutting down EndpointInterchange")

        # TODO: shut down executors gracefully

        # kill_event must be set before quiesce_event because we need to guarantee that
        # once the quiesce is complete, the interchange will not try to start again
        self._kill_event.set()
        self._quiesce_event.set()

    def handle_sigterm(self, sig_num, curr_stack_frame):
        log.warning("Received SIGTERM, attempting to save unacked results to disk")
        try:
            self.results_ack_handler.persist()
        except Exception:
            log.exception("Caught exception while saving unacked results")
        else:
            log.info("Unacked results successfully saved to disk")
        sys.exit(1)

    def start(self):
        """Start the Interchange"""
        log.info("Starting EndpointInterchange")

        signal.signal(signal.SIGTERM, self.handle_sigterm)

        self._quiesce_event.clear()
        self._kill_event.clear()

        # NOTE: currently we only start the executors once because
        # the current behavior is to keep them running decoupled while
        # the endpoint is waiting for reconnection
        self.start_executors()

        while not self._kill_event.is_set():
            self._start_threads_and_main()
            self.quiesce()
            # this check is solely for testing to force this loop to only run once
            if self._test_start:
                break

        log.info("EndpointInterchange shutdown complete.")

    def _start_threads_and_main(self):
        # re-register on every loop start
        if not self.initial_registration_complete:
            # Register the endpoint
            log.info("Running endpoint registration retry loop")
            reg_info = retry_call(
                self.register_endpoint, delay=10, max_delay=300, backoff=1.2
            )
            log.info(
                "Endpoint registered with UUID: {}".format(reg_info["endpoint_id"])
            )

        self.initial_registration_complete = False

        log.info(
            "Attempting connection to client at {} on ports: {},{},{}".format(
                self.client_address,
                self.client_ports[0],
                self.client_ports[1],
                self.client_ports[2],
            )
        )

        self._task_puller_thread = threading.Thread(
            target=self.migrate_tasks_to_internal, args=(self._quiesce_event,)
        )
        self._task_puller_thread.start()

        self._command_thread = threading.Thread(
            target=self._command_server, args=(self._quiesce_event,)
        )
        self._command_thread.start()

        try:
            self._main_loop()
        except Exception:
            log.exception("[MAIN] Unhandled exception")
        finally:
            self.results_outgoing.close()
            log.info("[MAIN] Thread loop exiting")

    def _main_loop(self):
        self.results_outgoing = TaskQueue(
            self.client_address,
            port=self.client_ports[1],
            identity=self.endpoint_id,
            mode="client",
            keys_dir=self.keys_dir,
            # Fail immediately if results cannot be sent back
            SNDTIMEO=0,
            set_hwm=True,
            linger=0,
        )
        self.results_outgoing.put(
            "forwarder", pickle.dumps({"registration": self.endpoint_id})
        )

        # TODO: this resend must happen after any endpoint re-registration to
        # ensure there are not unacked results left
        resend_results_messages = self.results_ack_handler.get_unacked_results_list()
        if len(resend_results_messages) > 0:
            log.info(
                "[MAIN] Resending %s previously unacked results",
                len(resend_results_messages),
            )

        # TODO: this should be a multipart send rather than a loop
        for results in resend_results_messages:
            self.results_outgoing.put("forwarder", results)

        executor = list(self.executors.values())[0]
        last = time.time()

        while not self._quiesce_event.is_set():
            if last + self.heartbeat_threshold < time.time():
                log.debug("[MAIN] alive")
                last = time.time()
                try:
                    # Adding results heartbeat to essentially force a TCP keepalive
                    # without meddling with OS TCP keepalive defaults
                    self.results_outgoing.put("forwarder", b"HEARTBEAT")
                except Exception:
                    log.exception(
                        "[MAIN] Sending heartbeat to the forwarder over the results "
                        "channel has failed"
                    )
                    raise

            self.results_ack_handler.check_ack_counts()

            try:
                task = self.pending_task_queue.get(block=True, timeout=0.01)
                executor.submit_raw(task.pack())
            except queue.Empty:
                pass
            except Exception:
                log.exception("[MAIN] Unhandled issue while waiting for pending tasks")

            try:
                results = self.results_passthrough.get(False, 0.01)

                task_id = results["task_id"]
                if task_id:
                    self.results_ack_handler.put(task_id, results["message"])
                    log.info(f"Passing result to forwarder for task {task_id}")

                # results will be a pickled dict with task_id, container_id,
                # and results/exception
                self.results_outgoing.put("forwarder", results["message"])

            except queue.Empty:
                pass

            except Exception:
                log.exception(
                    "[MAIN] Something broke while forwarding results from executor "
                    "to forwarder queues"
                )
                continue

    def get_status_report(self):
        """Get utilization numbers"""
        total_cores = 0
        total_mem = 0
        core_hrs = 0
        active_managers = 0
        free_capacity = 0
        outstanding_tasks = self.get_total_tasks_outstanding()
        pending_tasks = self.total_pending_task_count
        num_managers = len(self._ready_manager_queue)
        live_workers = self.get_total_live_workers()

        for manager in self._ready_manager_queue:
            total_cores += self._ready_manager_queue[manager]["cores"]
            total_mem += self._ready_manager_queue[manager]["mem"]
            active_dur = abs(
                time.time() - self._ready_manager_queue[manager]["reg_time"]
            )
            core_hrs += (active_dur * total_cores) / 3600
            if self._ready_manager_queue[manager]["active"]:
                active_managers += 1
            free_capacity += self._ready_manager_queue[manager]["free_capacity"][
                "total_workers"
            ]

        result_package = {
            "task_id": -2,
            "info": {
                "total_cores": total_cores,
                "total_mem": total_mem,
                "new_core_hrs": core_hrs - self.last_core_hr_counter,
                "total_core_hrs": round(core_hrs, 2),
                "managers": num_managers,
                "active_managers": active_managers,
                "total_workers": live_workers,
                "idle_workers": free_capacity,
                "pending_tasks": pending_tasks,
                "outstanding_tasks": outstanding_tasks,
                "worker_mode": self.config.worker_mode,
                "scheduler_mode": self.config.scheduler_mode,
                "scaling_enabled": self.config.scaling_enabled,
                "mem_per_worker": self.config.mem_per_worker,
                "cores_per_worker": self.config.cores_per_worker,
                "prefetch_capacity": self.config.prefetch_capacity,
                "max_blocks": self.config.provider.max_blocks,
                "min_blocks": self.config.provider.min_blocks,
                "max_workers_per_node": self.config.max_workers_per_node,
                "nodes_per_block": self.config.provider.nodes_per_block,
            },
        }

        self.last_core_hr_counter = core_hrs
        return result_package

    def scale_out(self, blocks=1, task_type=None):
        """Scales out the number of blocks by "blocks"

        Raises:
             NotImplementedError
        """
        r = []
        for _i in range(blocks):
            if self.config.provider:
                self._block_counter += 1
                external_block_id = str(self._block_counter)
                if not task_type and self.config.scheduler_mode == "hard":
                    launch_cmd = self.launch_cmd.format(
                        block_id=external_block_id, worker_type="RAW"
                    )
                else:
                    launch_cmd = self.launch_cmd.format(
                        block_id=external_block_id, worker_type=task_type
                    )
                if not task_type:
                    internal_block = self.config.provider.submit(launch_cmd, 1)
                else:
                    internal_block = self.config.provider.submit(
                        launch_cmd, 1, task_type
                    )
                log.debug(f"Launched block {external_block_id}->{internal_block}")
                if not internal_block:
                    raise (
                        ScalingFailed(
                            self.provider.label,
                            "Attempts to provision nodes via provider has failed",
                        )
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
                "Scaling in blocks of specific task type %s. "
                "Let the provider decide which to kill",
                task_type,
            )
            if self.config.scaling_enabled and self.config.provider:
                to_kill, r = self.config.provider.cancel(blocks, task_type)
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

        if self.config.scaling_enabled and self.config.provider:
            r = self.config.provider.cancel(to_kill)

        return r

    def provider_status(self):
        """Get status of all blocks from the provider"""
        status = []
        if self.config.provider:
            log.debug(
                "[MAIN] Getting the status of {} blocks.".format(
                    list(self.blocks.values())
                )
            )
            status = self.config.provider.status(list(self.blocks.values()))
            log.debug(f"[MAIN] The status is {status}")

        return status


def starter(comm_q, *args, **kwargs):
    """Start the interchange process

    The executor is expected to call this function.
    The args, kwargs match that of the Interchange.__init__
    """
    # logger = multiprocessing.get_logger()
    ic = EndpointInterchange(*args, **kwargs)
    # comm_q.put((ic.worker_task_port,
    #            ic.worker_result_port))
    ic.start()


def cli_run():

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
        "--worker_ports",
        default=None,
        help="OPTIONAL, pair of workers ports to listen on, "
        "e.g. --worker_ports=50001,50005",
    )
    parser.add_argument(
        "--suppress_failure",
        action="store_true",
        help="Enables suppression of failures",
    )
    parser.add_argument(
        "--endpoint_id",
        required=True,
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

    logdir = os.path.abspath(args.logdir)
    os.makedirs(logdir, exist_ok=True)
    setup_logging(logfile=os.path.join(logdir, "endpoint.log"), debug=args.debug)

    optionals = {}
    optionals["suppress_failure"] = args.suppress_failure
    optionals["logdir"] = os.path.abspath(args.logdir)
    optionals["client_address"] = args.client_address
    optionals["client_ports"] = [int(i) for i in args.client_ports.split(",")]
    optionals["endpoint_id"] = args.endpoint_id

    # DEBUG ONLY : TODO: FIX
    if args.config is None:
        from parsl.providers import LocalProvider

        from funcx_endpoint.endpoint.utils.config import Config

        config = Config(
            worker_debug=True,
            scaling_enabled=True,
            provider=LocalProvider(
                init_blocks=1,
                min_blocks=1,
                max_blocks=1,
            ),
            max_workers_per_node=2,
            funcx_service_address="http://127.0.0.1:8080",
        )
        optionals["config"] = config
    else:
        optionals["config"] = args.config

    if args.worker_ports:
        optionals["worker_ports"] = [int(i) for i in args.worker_ports.split(",")]
    if args.worker_port_range:
        optionals["worker_port_range"] = [
            int(i) for i in args.worker_port_range.split(",")
        ]

    ic = EndpointInterchange(**optionals)
    ic.start()

    """
    with daemon.DaemonContext():
    """
