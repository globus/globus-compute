from __future__ import annotations

import logging
import multiprocessing
import os
import platform
import queue
import random
import signal
import sys
import threading
import time

# multiprocessing.Event is a method, not a class
# to annotate, we need the "real" class
# see: https://github.com/python/typeshed/issues/4266
from multiprocessing.synchronize import Event as EventType

import pika.exceptions
from funcx_common.messagepack import pack
from funcx_common.messagepack.message_types import Result, ResultErrorDetails
from parsl.version import VERSION as PARSL_VERSION

import funcx_endpoint.endpoint.utils.config
from funcx import __version__ as funcx_sdk_version
from funcx.sdk.client import FuncXClient
from funcx_endpoint import __version__ as funcx_endpoint_version
from funcx_endpoint.endpoint.messages_compat import (
    try_convert_from_messagepack,
    try_convert_to_messagepack,
)
from funcx_endpoint.endpoint.rabbit_mq import ResultQueuePublisher, TaskQueueSubscriber
from funcx_endpoint.endpoint.result_store import ResultStore
from funcx_endpoint.executors.high_throughput.mac_safe_queue import mpQueue

log = logging.getLogger(__name__)


class EndpointInterchange:
    """Interchange is a task orchestrator for distributed systems.

    1. Asynchronously queue large volume of tasks (>100K)
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    4. Service single and batch requests from workers
    5. Be aware of requests worker resource capacity,
       eg. schedule only jobs that fit into walltime.

    """

    def __init__(
        self,
        config: funcx_endpoint.endpoint.utils.config.Config,
        reg_info: dict[str, dict],
        logdir=".",
        endpoint_id=None,
        endpoint_dir=".",
        funcx_client: FuncXClient | None = None,
        result_store: ResultStore | None = None,
        reconnect_attempt_limit: int = 5,
    ):
        """
        Parameters
        ----------
        config : funcx.Config object
             Funcx config object that describes how compute should be provisioned

        reg_info : dict[str, dict]
             Dictionary containing connection information for both the task and
             result queues.  The required data structure is returned from the
             Endpoint registration API call, encapsulated in the SDK by
             `FuncXClient.register_endpoint()`.

        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'

        endpoint_id : str
             Identity string that identifies the endpoint to the broker

        endpoint_dir : pathlib.Path
             Endpoint directory path to store registration info in

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

        self.endpoint_dir = endpoint_dir

        if funcx_client is None:
            funcx_client = FuncXClient()
        self.funcx_client = funcx_client

        self.task_q_info = reg_info["task_queue_info"]
        self.result_q_info = reg_info["result_queue_info"]

        self.time_to_quit = False
        self.heartbeat_period = self.config.heartbeat_period

        self.pending_task_queue: multiprocessing.Queue = multiprocessing.Queue()

        self._reconnect_fail_counter = 0
        self.reconnect_attempt_limit = max(1, reconnect_attempt_limit)
        self._quiesce_event = multiprocessing.Event()
        self._kill_event = multiprocessing.Event()

        if result_store is None:
            result_store = ResultStore(endpoint_dir=endpoint_dir)
        self.result_store = result_store

        self.endpoint_id = endpoint_id

        self.current_platform = {
            "parsl_v": PARSL_VERSION,
            "python_v": "{}.{}.{}".format(
                sys.version_info.major, sys.version_info.minor, sys.version_info.micro
            ),
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

        self._test_start = False

    def load_config(self):
        """Load the config"""
        log.info("Loading endpoint local config")

        self.results_passthrough = mpQueue()
        self.executors: dict[str, funcx_endpoint.executors.HighThroughputExecutor] = {}
        for executor in self.config.executors:
            log.info(f"Initializing executor: {executor.label}")
            executor.funcx_service_address = self.config.funcx_service_address
            if not executor.endpoint_id:
                executor.endpoint_id = self.endpoint_id
            else:
                if not executor.endpoint_id == self.endpoint_id:
                    eep_id = f"Executor({executor.endpoint_id})"
                    sep_id = f"Interchange({self.endpoint_id})"
                    raise Exception(f"InconsistentEndpointId: {eep_id} != {sep_id}")
            self.executors[executor.label] = executor
            if executor.run_dir is None:
                executor.run_dir = self.logdir

    def start_executors(self):
        log.info("Starting Executors")
        for executor in self.config.executors:
            if hasattr(executor, "passthrough") and executor.passthrough is True:
                executor.start(
                    results_passthrough=self.results_passthrough,
                    funcx_client=self.funcx_client,
                )

    def migrate_tasks_to_internal(
        self,
        connection_params: dict,
        endpoint_uuid: str,
        pending_task_queue: multiprocessing.Queue,
        quiesce_event: EventType,
    ) -> multiprocessing.Process:
        """Pull tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue

        Parameters:
        -----------
        connection_params: pika.connection.Parameters
              Connection params to connect to the service side Tasks queue

        endpoint_uuid: endpoint_uuid str

        pending_task_queue: multiprocessing.Queue
              Internal queue to which tasks should be migrated

        quiesce_event : EventType
              Event to let the thread know when it is time to die.
        """
        try:
            log.info(f"Starting the TaskQueueSubscriber as {endpoint_uuid}")
            task_q_proc = TaskQueueSubscriber(
                queue_info=connection_params,
                external_queue=pending_task_queue,
                quiesce_event=quiesce_event,
                endpoint_id=endpoint_uuid,
            )
            task_q_proc.start()
        except Exception:
            log.exception("Unhandled exception in TaskQueueSubscriber")
            raise

        return task_q_proc

    def quiesce(self):
        """Temporarily stop everything on the interchange in order to reach a consistent
        state before attempting to start again. This must be called on the main thread
        """
        log.info("Interchange Quiesce in progress (stopping and joining processes)")
        self._quiesce_event.set()

        log.info("Waiting for quiesce complete")
        self._task_puller_proc.join()

        log.info("Quiesce done")

    def stop(self):
        """Prepare the interchange for shutdown"""
        log.info("Shutting down EndpointInterchange")

        # kill_event must be set before quiesce_event because we need to guarantee that
        # once the quiesce is complete, the interchange will not try to start again
        self._kill_event.set()
        self._quiesce_event.set()

    def cleanup(self):
        for label in self.executors:
            self.executors[label].shutdown()

    def handle_sigterm(self, sig_num, curr_stack_frame):
        log.warning("Received SIGTERM, setting termination flag.")
        self.time_to_quit = True

    def start(self):
        """Start the Interchange"""
        log.info("Starting EndpointInterchange")

        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGQUIT, self.handle_sigterm)  # hint: Ctrl+\
        # Intentionally ignoring SIGINT for now, as we're unstable enough to
        # warrant Python's default developer-friendly Ctrl+C handling

        self._quiesce_event.clear()
        self._kill_event.clear()

        # NOTE: currently we only start the executors once because
        # the current behavior is to keep them running decoupled while
        # the endpoint is waiting for reconnection
        self.start_executors()

        while not self._kill_event.is_set():
            if self._reconnect_fail_counter >= self.reconnect_attempt_limit:
                log.critical(
                    f"Failed {self._reconnect_fail_counter} consecutive times."
                    "  Shutting down."
                )
                self.stop()
                self.quiesce()
                break

            if self._quiesce_event.is_set():
                idle_for = random.uniform(2.0, 10.0)
                log.warning(f"Interchange will retry connecting in {idle_for:.2f}s")
                time.sleep(idle_for)
                self._quiesce_event.clear()
            else:
                log.debug("Starting threads and main loop")

            if self.time_to_quit:
                self.stop()
                self.quiesce()
                break

            try:
                self._start_threads_and_main()
            except pika.exceptions.ProbableAuthenticationError as e:
                log.error(f"Unable to connect to AMQP service: {e}")
                self._kill_event.set()
            except Exception:
                self._reconnect_fail_counter += 1
                log.exception("Unhandled exception in main kernel.")
                log.info(
                    "Reconnection count: %s (of %s)",
                    self._reconnect_fail_counter,
                    self.reconnect_attempt_limit,
                )

            self.quiesce()

        self.cleanup()
        log.info("EndpointInterchange shutdown complete.")

    def _start_threads_and_main(self):
        self._task_puller_proc = self.migrate_tasks_to_internal(
            self.task_q_info,
            self.endpoint_id,
            self.pending_task_queue,
            self._quiesce_event,
        )

        self._main_loop()

    def _main_loop(self):
        """
        This is the "kernel" of the endpoint interchange process.  Conceptually, there
        are three actions of consequence: forward task messages to the executors,
        forward results from the executors back to the funcx web services (RMQ), and
        forward any previous results that may have failed to send previously (e.g., if
        a RMQ connection was dropped).

        We accomplish this via three threads, one each for each task.

        Of special note is that this kernel does not try very hard to handle the
        non-happy path.  If an error occurs that is too "unhappy" (e.g., communication
        with RMQ fails), the catch-all solution is to "reboot" and try again.  That is,
        this method exits, communication is shutdown, started again, and we restart
        this method.
        """
        log.debug("_main_loop begins")

        results_publisher = ResultQueuePublisher(
            queue_info=self.result_q_info,
        )

        with results_publisher:
            executor = list(self.executors.values())[0]

            num_tasks_forwarded = 0
            num_results_forwarded = 0

            def process_stored_results():
                # Handle any previously stored results, either from a previous run or
                # from a quarter of a second-ago.  Basically, check every second
                # (.wait()) if there are any items on disk to be sent.  If there are,
                # don't treat them any differently than "fresh" results: put the into
                # the same multiprocessing queue as results incoming directly from
                # the executors.  The normal processing by `process_pending_results()`
                # will take over from there.
                while not self._quiesce_event.wait(timeout=1):
                    for task_id, packed_result in self.result_store:
                        if self._quiesce_event.is_set():
                            # important to check every iteration as well, so as not to
                            # potentially hang up the shutdown procedure
                            return
                        log.debug("Retrieved stored result (%s)", task_id)
                        msg = {"task_id": task_id, "message": packed_result}
                        self.result_store.discard(task_id)
                        self.results_passthrough.put(msg)
                log.debug("Exit process-stored-results thread.")

            def process_pending_tasks():
                # Pull tasks from upstream (RMQ) and send them down the ZMQ pipe to the
                # funcx-manager.  In terms of shutting down (or "rebooting") gracefully,
                # iterate once a second whether or not a task has arrived.
                nonlocal num_tasks_forwarded
                while not self._quiesce_event.is_set():
                    if self.time_to_quit:
                        self.stop()
                        continue  # nominally == break; but let event do it

                    try:
                        incoming_task = self.pending_task_queue.get(timeout=1)
                        task = try_convert_from_messagepack(incoming_task)
                        executor.submit_raw(task)
                        num_tasks_forwarded += 1  # Safe given GIL

                    except queue.Empty:
                        continue

                    except Exception:
                        log.exception("Unhandled issue while waiting for pending tasks")

                log.debug("Exit process-pending-tasks thread.")

            def process_pending_results():
                # Forward incoming results from the funcx-manager to the funcx-services.
                # For graceful handling of shutdown (or "reboot"), wait up to a second
                # for incoming results before iterating the loop regardless.
                nonlocal num_results_forwarded
                while not self._quiesce_event.is_set():
                    try:
                        result = self.results_passthrough.get(timeout=1)
                        task_id = result["task_id"]
                        packed_result = result["message"]

                    except queue.Empty:
                        # Empty queue!  Let's see if we have any prior results to send
                        continue

                    except Exception as exc:
                        log.warning(
                            f"Invalid message received: no task_id.  Ignoring. {exc}"
                        )
                        continue

                    try:
                        # This either works or it doesn't; if it doesn't to serialize
                        # the to an execption and send _that_
                        # will be a packed EPStatusReport or Result
                        message = try_convert_to_messagepack(packed_result)

                    except Exception as exc:
                        log.exception(
                            f"Unable to parse result message for task {task_id}."
                            "   Marking task as failed."
                        )

                        kwargs = {
                            "task_id": task_id,
                            "data": packed_result,
                            "error_details": ResultErrorDetails(
                                code=0,
                                user_message=(
                                    "Endpoint failed to serialize."
                                    f"  Exception text: {exc}"
                                ),
                            ),
                        }
                        message = pack(Result(**kwargs))

                    if task_id:
                        log.debug(f"Forwarding result for task {task_id}")

                    try:
                        results_publisher.publish(message)
                        num_results_forwarded += 1  # Safe given GIL

                    except Exception:
                        # Publishing didn't work -- quiesce and see if a simple restart
                        # fixes the issue.
                        self._quiesce_event.set()

                        log.exception("Something broke while forwarding results")
                        if task_id:
                            log.info("Storing result for later: %s", task_id)
                            self.result_store[task_id] = packed_result
                        continue  # just be explicit

                log.debug("Exit process-pending-results thread.")

            stored_processor_thread = threading.Thread(
                target=process_stored_results, name="Stored Result Handler"
            )
            task_processor_thread = threading.Thread(
                target=process_pending_tasks, name="Pending Task Handler"
            )
            result_processor_thread = threading.Thread(
                target=process_pending_results, name="Pending Result Handler"
            )
            stored_processor_thread.start()
            task_processor_thread.start()
            result_processor_thread.start()

            log.debug("_main_loop entered running state")
            last_t, last_r = 0, 0
            while not self._quiesce_event.wait(self.heartbeat_period):
                # Possibly TOCTOU here, but we don't need to be super precise.  The
                # point here is to mention "still alive" and that we're still working
                num_t, num_r = num_tasks_forwarded, num_results_forwarded
                log.debug(
                    "Heartbeat.  Approximate Tasks and Results forwarded since last "
                    "heartbeat: %s (T), %s (R)",
                    num_t - last_t,
                    num_r - last_r,
                )
                last_t, last_r = num_t, num_r

                # only reset come heartbeat and still alive
                self._reconnect_fail_counter = 0

            # The timeouts aren't nominally necessary because if the above loop has
            # quit, then the _quiesce_event is set, and both threads check that event
            # every internal iteration.  But "for kicks."
            stored_processor_thread.join(timeout=5)
            task_processor_thread.join(timeout=5)
            result_processor_thread.join(timeout=5)
