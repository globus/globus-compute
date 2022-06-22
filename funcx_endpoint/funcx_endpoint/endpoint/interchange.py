import logging
import multiprocessing
import os
import pickle
import platform
import queue
import signal
import sys
import time
import typing as t

# multiprocessing.Event is a method, not a class
# to annotate, we need the "real" class
# see: https://github.com/python/typeshed/issues/4266
from multiprocessing.synchronize import Event as EventType
from typing import Dict

from parsl.version import VERSION as PARSL_VERSION
from retry.api import retry_call

import funcx_endpoint.endpoint.utils.config
from funcx import __version__ as funcx_sdk_version
from funcx.sdk.client import FuncXClient
from funcx_endpoint import __version__ as funcx_endpoint_version
from funcx_endpoint.endpoint.messages_compat import (
    try_convert_from_messagepack,
    try_convert_to_messagepack,
)
from funcx_endpoint.endpoint.rabbit_mq import ResultQueuePublisher, TaskQueueSubscriber
from funcx_endpoint.endpoint.register_endpoint import register_endpoint
from funcx_endpoint.executors.high_throughput.mac_safe_queue import mpQueue

log = logging.getLogger(__name__)

LOOP_SLOWDOWN = 0.0  # in seconds
HEARTBEAT_CODE = (2**32) - 1
PKL_HEARTBEAT_CODE = pickle.dumps(HEARTBEAT_CODE)


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
        reg_info: t.Tuple[t.Dict, t.Dict] = None,
        logdir=".",
        endpoint_id=None,
        endpoint_dir=".",
        endpoint_name="default",
        funcx_client_options=None,
        results_ack_handler=None,
    ):
        """
        Parameters
        ----------
        config : funcx.Config object
             Funcx config object that describes how compute should be provisioned

        reg_info : tuple[Dict, Dict]
             Tuple of connection info for task_queue and result_queue
             Connection parameters to connect to the service side RabbitMQ pipes
             Optional: If not supplied, the endpoint will use a retry loop to
             attempt registration periodically.

        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'

        endpoint_id : str
             Identity string that identifies the endpoint to the broker

        endpoint_dir : str
             Endpoint directory path to store registration info in

        endpoint_name : str
             Name of endpoint

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

        self.endpoint_dir = endpoint_dir
        self.endpoint_name = endpoint_name

        if funcx_client_options is None:
            funcx_client_options = {}
        # off_process checker is only needed on client side
        funcx_client_options["use_offprocess_checker"] = False
        self.funcx_client = FuncXClient(**funcx_client_options)

        self.initial_registration_complete = False
        self.task_q_info, self.result_q_info = None, None
        if reg_info:
            self.task_q_info, self.result_q_info = reg_info
            self.initial_registration_complete = True

        self.heartbeat_period = self.config.heartbeat_period
        self.heartbeat_threshold = self.config.heartbeat_threshold

        self.pending_task_queue: multiprocessing.Queue = multiprocessing.Queue()

        self._quiesce_event = multiprocessing.Event()
        self._kill_event = multiprocessing.Event()

        self.results_ack_handler = results_ack_handler

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
        self.executors: Dict[str, funcx_endpoint.executors.HighThroughputExecutor] = {}
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

    def register_endpoint(self):
        reg_info = register_endpoint(
            self.funcx_client, self.endpoint_id, self.endpoint_dir, self.endpoint_name
        )
        self.task_q_info, self.result_q_info = reg_info

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

        quiesce_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        log.info("Starting")

        try:
            log.info(
                f"[TASK_PULL_PROC Starting the TaskQueueSubscriber"
                f" as {endpoint_uuid}"
            )
            task_q_proc = TaskQueueSubscriber(
                queue_info=connection_params,
                external_queue=pending_task_queue,
                kill_event=quiesce_event,
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

        log.info("Saving unacked results to disk")
        try:
            self.results_ack_handler.persist()
        except Exception:
            log.exception("Caught exception while saving unacked results")
            log.warning("Interchange will continue without saving unacked results")
        log.info("Waiting for quiesce complete")

        self._task_puller_proc.join()

        log.info("Quiesce done")
        # this must be called last to ensure the next interchange run will occur
        self._quiesce_event.clear()

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
        log.warning("Received SIGTERM, attempting to save unacked results to disk")
        try:
            self.stop()
        except Exception:
            log.exception("Caught exception while saving unacked results")
        else:
            log.info("Unacked results successfully saved to disk")
        # sys.exit(1)

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
            try:
                self._start_threads_and_main()
            except Exception:
                log.exception("Unhandled exception in main kernel.")

            self.quiesce()
            # this check is solely for testing to force this loop to only run once
            if self._test_start:
                break

        self.cleanup()
        log.info("EndpointInterchange shutdown complete.")

    def _start_threads_and_main(self):
        # re-register on every loop start
        if not self.initial_registration_complete:
            # Register the endpoint
            log.info("Running endpoint registration retry loop")
            retry_call(self.register_endpoint, delay=10, max_delay=300, backoff=1.2)
            log.info("Endpoint registered with UUID: (FIXME FIXME FIXME)")

        self.initial_registration_complete = False

        self._task_puller_proc = self.migrate_tasks_to_internal(
            self.task_q_info,
            self.endpoint_id,
            self.pending_task_queue,
            self._quiesce_event,
        )

        self._main_loop()

    def _main_loop(self):
        results_publisher = ResultQueuePublisher(
            queue_info=self.result_q_info,
        )

        with results_publisher.connect():
            # TODO: this resend must happen after any endpoint re-registration to
            # ensure there are not unacked results left
            resend_results_messages = (
                self.results_ack_handler.get_unacked_results_list()
            )
            if len(resend_results_messages) > 0:
                log.info(
                    "Resending %s previously unacked results",
                    len(resend_results_messages),
                )

            for results in resend_results_messages:
                # TO-DO: Republishing backlogged/unacked messages is not supported
                # until the types are sorted out
                results_publisher.publish(try_convert_to_messagepack(results))

            executor = list(self.executors.values())[0]
            last = time.time()

            while not self._quiesce_event.is_set() and not self._kill_event.is_set():
                if last + self.heartbeat_period < time.time():
                    log.debug("alive")
                    last = time.time()

                self.results_ack_handler.check_ack_counts()

                try:
                    incoming_task = self.pending_task_queue.get(
                        block=True, timeout=0.01
                    )
                    task = try_convert_from_messagepack(incoming_task)
                    log.warning(f"Submitting task : {task}")
                    executor.submit_raw(task)
                except queue.Empty:
                    pass
                except Exception:
                    log.exception("Unhandled issue while waiting for pending tasks")

                try:
                    results = self.results_passthrough.get(False, 0.01)

                    task_id = results["task_id"]
                    if task_id:
                        self.results_ack_handler.put(task_id, results["message"])
                        log.info(f"Passing result to forwarder for task {task_id}")

                    message = try_convert_to_messagepack(results["message"])

                    # results will be a packed EPStatusReport or a packed Result
                    log.warning(f"Publishing message {message}")
                    results_publisher.publish(message)
                except queue.Empty:
                    pass

                except Exception:
                    log.exception(
                        "Something broke while forwarding results from executor "
                        "to forwarder queues"
                    )
                    continue
