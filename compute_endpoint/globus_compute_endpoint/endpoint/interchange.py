from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import platform
import queue
import random
import signal
import threading
import time
import typing as t
import uuid
from concurrent.futures import Future

import pika.exceptions
import setproctitle
from globus_compute_common.messagepack import InvalidMessageError, pack
from globus_compute_common.messagepack.message_types import Result, ResultErrorDetails
from globus_compute_common.tasks import TaskState
from globus_compute_endpoint import __version__ as funcx_endpoint_version
from globus_compute_endpoint.endpoint.config import UserEndpointConfig
from globus_compute_endpoint.endpoint.rabbit_mq import (
    ResultPublisher,
    TaskQueueSubscriber,
)
from globus_compute_endpoint.endpoint.result_store import ResultStore
from globus_compute_endpoint.engines.base import GCFuture, GlobusComputeEngineBase
from globus_compute_endpoint.exception_handling import get_result_error_details
from globus_compute_sdk import __version__ as funcx_sdk_version
from globus_compute_sdk.sdk.utils import get_py_version_str
from globus_compute_sdk.sdk.utils.uuid_like import UUID_LIKE_T
from parsl.version import VERSION as PARSL_VERSION

log = logging.getLogger(__name__)


class _ResultPassthroughType(t.TypedDict):
    message: bytes
    task_id: str


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
        config: UserEndpointConfig,
        reg_info: dict[str, dict],
        ep_info: dict,
        logdir=".",
        endpoint_id=None,
        endpoint_dir=".",
        result_store: ResultStore | None = None,
        reconnect_attempt_limit: int = 5,
        parent_pid: int = 0,
        audit_fd: int | None = None,
    ):
        """
        Parameters
        ----------
        config : globus_compute_sdk.UserEndpointConfig object
             Globus Compute config object describing how compute should be provisioned

        reg_info : dict[str, dict]
             Dictionary containing connection information for the task, result, and
             heartbeat queues.  The required data structure is returned from the
             Endpoint registration API call, encapsulated in the SDK by
             `Client.register_endpoint()`.

        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'

        endpoint_id : str
             Identity string that identifies the endpoint to the broker

        endpoint_dir : pathlib.Path
             Endpoint directory path to store registration info in
        """
        self.logdir = logdir
        log.info(
            "Initializing EndpointInterchange process with Endpoint ID: {}".format(
                endpoint_id
            )
        )
        self.config = config
        if self.config.engine is None:
            raise ValueError("No Compute Engine specified")

        self._audit_fd = audit_fd
        self.endpoint_dir = endpoint_dir

        self.task_q_info = reg_info["task_queue_info"]
        self.result_q_info = reg_info["result_queue_info"]
        self.heartbeat_q_info = reg_info["heartbeat_queue_info"]

        self.time_to_quit = False
        self.heartbeat_period = self.config.heartbeat_period

        self.pending_task_queue: queue.SimpleQueue = queue.SimpleQueue()

        self._ep_info = ep_info
        self._reconnect_fail_counter = 0
        self.reconnect_attempt_limit = max(1, reconnect_attempt_limit)
        self._quiesce_event = threading.Event()
        self._parent_pid = parent_pid

        if result_store is None:
            result_store = ResultStore(endpoint_dir=endpoint_dir)
        self.result_store = result_store

        self.endpoint_id = endpoint_id

        self.current_platform = {
            "parsl_v": PARSL_VERSION,
            "python_v": get_py_version_str(),
            "os": platform.system(),
            "hname": platform.node(),
            "funcx_sdk_version": funcx_sdk_version,
            "funcx_endpoint_version": funcx_endpoint_version,
            "registration": self.endpoint_id,
            "dir": os.getcwd(),
        }
        log.info(f"Platform info: {self.current_platform}")

        self.engine: GlobusComputeEngineBase = self.config.engine

    def start_engine(self):
        log.info("Starting Engine")
        self.engine.start(
            endpoint_id=self.endpoint_id,
            run_dir=self.logdir,
        )

    def quiesce(self):
        """Temporarily stop everything on the interchange in order to reach a consistent
        state before attempting to start again. This must be called on the main thread
        """
        log.info("Interchange quiesce in progress")
        self._quiesce_event.set()

    def stop(self):
        """Prepare the interchange for shutdown"""
        log.info("Shutting down EndpointInterchange")

        # set `time_to_quit` prior to `quiesce_event` to ensure that the interchange
        # will not start again after quiesce completes
        self.time_to_quit = True
        self._quiesce_event.set()

    def cleanup(self):
        self.engine.shutdown(block=True)
        if self._audit_fd:
            try:
                os.close(self._audit_fd)
            except Exception:
                log.debug("Unknown problem closing audit log", exc_info=True)
            finally:
                self._audit_fd = None

    def handle_sigterm(self, sig_num, curr_stack_frame):
        log.warning("Received SIGTERM, setting termination flag.")
        self.stop()

    def function_allowed(self, function_id: UUID_LIKE_T):
        if self.config.allowed_functions is None:
            # Not a restricted endpoint
            return True

        return str(function_id) in self.config.allowed_functions

    def start(self):
        """Start the Interchange"""
        if self.time_to_quit:
            # Reminder to the dev: create afresh, don't reuse
            log.debug("Interchange object has stopped; it cannot be restarted")
            return

        signal.signal(signal.SIGHUP, self.handle_sigterm)
        signal.signal(signal.SIGQUIT, self.handle_sigterm)  # hint: Ctrl+\
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        # Intentionally ignoring SIGINT for now, as we're unstable enough to
        # warrant Python's default developer-friendly Ctrl+C handling

        self._quiesce_event.clear()

        if self._parent_pid:
            if self._parent_pid != os.getppid():
                # initial check to save extra noise
                log.warning(f"Pid {self._parent_pid} not parent; refusing to start")
                self.stop()
                return

            def _parent_watcher(ppid: int):
                while ppid == os.getppid():
                    if self._quiesce_event.wait(timeout=1):
                        return
                log.warning(f"Parent ({ppid}) has gone away; initiating shut down")
                self.stop()

            threading.Thread(
                target=_parent_watcher,
                args=(self._parent_pid,),  # copy; to discourage shenanigans
                daemon=True,
                name="ParentPidWatcher",
            ).start()

        log.info("Starting EndpointInterchange")

        # NOTE: currently we only start the engine once because the current behavior
        # is to keep it running decoupled while the endpoint is waits for reconnection
        self.start_engine()

        while not self.time_to_quit:
            if self._reconnect_fail_counter >= self.reconnect_attempt_limit:
                log.critical(
                    f"Failed {self._reconnect_fail_counter} consecutive times."
                    "  Shutting down."
                )
                self.stop()
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
                break

            try:
                self._main_loop()
            except pika.exceptions.ConnectionClosedByBroker as e:
                log.warning(f"AMQP service closed connection: {e}")
            except pika.exceptions.ProbableAuthenticationError as e:
                log.error(f"Unable to connect to AMQP service: {e}")
                self.time_to_quit = True
            except Exception as e:
                log.error(
                    f"Unhandled exception in main kernel: ({type(e).__name__}) {e}"
                )
                log.debug("Unhandled exception in main kernel.", exc_info=e)
            finally:
                if not self.time_to_quit:
                    self._reconnect_fail_counter += 1
                    log.info(
                        "Reconnection count: %s (of %s)",
                        self._reconnect_fail_counter,
                        self.reconnect_attempt_limit,
                    )

            self.quiesce()

        self.cleanup()
        log.info("EndpointInterchange shutdown complete.")

    def audit(self, task_action: TaskState, task: GCFuture, msg: str = ""):
        """
        Write audit records (single line messages) to the audit file descriptor.

        Note that auditing is only available for High Assurance endpoints.

        :param task_action: The state of the task at the point of the audit call
        :param task: The associated task relevant to this audit message
        :param msg: Additional information to append to the audit log record;
            newlines (``\\r``, ``\\n``) and null charactors (``\\0``), if present,
            will be removed
        """
        if not (self.config.high_assurance and self._audit_fd):
            return

        tid = task.gc_task_id
        fid = task.function_id or ""
        bid = task.block_id or ""
        jid = task.job_id and f"jid={task.job_id} " or ""
        audit_msg = f"fid={fid} tid={tid} bid={bid} {jid}{task_action.name}"

        if msg:
            audit_msg += f" - {msg}"

        audit_msg = audit_msg.replace("\n", " ").replace("\r", "").replace("\0", "")
        try:
            os.write(self._audit_fd, audit_msg.encode())
        except Exception as e:
            # if we can't audit, disallow further processing
            self.stop()
            e_str = f"({type(e).__name__}) {e}"
            log.error(f"Unable to write audit log; endpoint may not continue: {e_str}")

    def _main_loop(self):
        """
        This is the "kernel" of the endpoint interchange process.  Conceptually, there
        are three actions of consequence: forward task messages to the engine, forward
        results from the engine back to the Globus Compute web services (RMQ), and
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

        task_q_subscriber = TaskQueueSubscriber(
            queue_info=self.task_q_info,
            pending_task_queue=self.pending_task_queue,
            thread_name="TQS",
        )
        task_q_subscriber.start()

        results_publisher = ResultPublisher(queue_info=self.result_q_info)
        results_publisher.start()

        heartbeat_publisher = ResultPublisher(queue_info=self.heartbeat_q_info)
        heartbeat_publisher.start()

        engine = self.engine

        num_tasks_received = 0
        num_results_forwarded = 0
        hb_lock = threading.Lock()

        def _mark_task_started(gcf: GCFuture, _new_val):
            self.audit(TaskState.EXEC_START, gcf)

        def _mark_task_running(gcf: GCFuture, _new_val):
            self.audit(TaskState.RUNNING, gcf)

        def process_stored_results():
            # Handle any previously stored results, either from a previous run or
            # from a quarter of a second-ago.  Basically, check every second
            # (.wait()) if there are any items on disk to be sent.  If there are,
            # don't treat them any differently than "fresh" results: put them into
            # the same queue as results coming directly from the engine.  The
            # normal processing by `process_pending_results()` will take over from
            # there.
            while not self._quiesce_event.wait(timeout=1):
                for task_id, packed_result in self.result_store:
                    log.debug("Retrieved stored result (%s)", task_id)
                    f = GCFuture(task_id)
                    f.add_done_callback(forward_result)
                    f.set_result(packed_result)
                    self.result_store.discard(task_id)

                    if self._quiesce_event.is_set():
                        # important to check every iteration as well, so as not to
                        # potentially hang up the shutdown procedure
                        return
            log.debug("Thread exit")

        def publish_done(task_id: uuid.UUID, task_res: bytes):
            def _done_cb(pub_fut: Future):
                _exc = pub_fut.exception()
                if _exc:
                    # Publishing didn't work -- quiesce and see if a simple restart
                    # fixes the issue.
                    log.error("Failed to publish results", exc_info=_exc)
                    log.info(f"Storing result for later: {task_id}")
                    self.result_store[str(task_id)] = task_res
                    self._quiesce_event.set()
                else:
                    nonlocal num_results_forwarded
                    num_results_forwarded += 1

            return _done_cb

        def forward_result(task_f: GCFuture) -> None:
            self.audit(TaskState.EXEC_END, task_f)
            try:
                res = task_f.result()
            except Exception as e:
                msg = f"Unknown task exception: ({type(e).__name__}) {e}"
                failed_result = Result(
                    task_id=task_f.gc_task_id,
                    data=msg,
                    error_details=ResultErrorDetails(
                        code="UNKNOWN_TASK_FAILURE", user_message=msg
                    ),
                    task_statuses=[],
                )
                res = pack(failed_result)

            try:
                f = results_publisher.publish(res)
                f.add_done_callback(publish_done(task_f.gc_task_id, res))

            except Exception:
                # Publishing didn't work -- quiesce and see if a simple restart fixes
                # the issue.
                self._quiesce_event.set()

                log.exception(
                    "Something broke while forwarding results; setting quiesce event"
                )
                log.info("Storing result for later: %s", task_f.gc_task_id)
                self.result_store[str(task_f.gc_task_id)] = res

        def process_pending_tasks() -> None:
            # Pull tasks from upstream (RMQ) and send them down the ZMQ pipe to the
            # globus-compute-manager.  In terms of shutting down (or "rebooting")
            # gracefully, iterate once a second whether a task has arrived.
            nonlocal num_tasks_received
            while True:
                try:
                    task = self.pending_task_queue.get()
                    if not task:  # poison pill; time to quit
                        break

                    d_tag, prop_headers, body = task
                    with hb_lock:
                        if self._quiesce_event.is_set():
                            # Important not to ACK; if only quiescing, then the next
                            # time the EP starts, AMQP will resend the task.
                            break

                        num_tasks_received += 1

                    fid: str = prop_headers.get("function_uuid")
                    tid: str = prop_headers.get("task_uuid")

                    if not fid or not tid:
                        raise InvalidMessageError(
                            "Task message missing function or task id in headers"
                        )

                    res_spec_s: str = (
                        prop_headers.get("resource_specification") or "null"
                    )
                    res_spec: dict = json.loads(res_spec_s) or {}
                    res_serde: list[str] = prop_headers.get("result_serializers") or []

                    task_f = GCFuture(tid, function_id=fid)
                    task_f.bind("block_id", _mark_task_running)
                    task_f.bind("executor_task_id", _mark_task_started)

                    task_f.add_done_callback(forward_result)  # type: ignore[arg-type]
                    self.audit(TaskState.RECEIVED, task_f)

                    if fid and not self.function_allowed(fid):
                        # Same as web-service message but packed in a
                        # result error
                        reject_msg = (
                            f"Function {fid} not permitted on "
                            f"endpoint {self.endpoint_id}"
                        )
                        log.warning(reject_msg)

                        audit_msg = "Function not permitted on endpoint"
                        self.audit(TaskState.FAILED, task_f, audit_msg)

                        failed_result = Result(
                            task_id=tid,
                            data=reject_msg,
                            details={"fid": fid, "eid": self.endpoint_id},
                            error_details=ResultErrorDetails(
                                code="FUNCTION_NOT_ALLOWED", user_message=reject_msg
                            ),
                        )
                        task_f.set_result(pack(failed_result))
                        del failed_result
                        continue

                except Exception:
                    log.exception("Unhandled error processing incoming task")
                    continue

                try:
                    engine.submit(
                        task_f=task_f,
                        packed_task=body,
                        resource_specification=res_spec,
                        result_serializers=res_serde,
                    )
                    task_q_subscriber.ack(d_tag)
                except Exception as exc:
                    log.exception(f"Failed to process task {tid}")
                    code, msg = get_result_error_details()
                    failed_result = Result(
                        task_id=tid,
                        data=f"Failed to start task: {exc}",
                        error_details=ResultErrorDetails(code=code, user_message=msg),
                        task_statuses=[],
                    )
                    task_f.set_result(pack(failed_result))

            log.debug("Thread exit")

        def heartbeat(active: bool = True) -> Future[None]:
            def _done_cb(pub_fut: Future):
                _exc = pub_fut.exception()
                if _exc:
                    # Publishing didn't work -- quiesce and see if a simple
                    # restart fixes the issue.
                    self._quiesce_event.set()
                    log.error("Failed to publish heartbeat", exc_info=_exc)

            try:
                sr = engine.get_status_report()
                sr.global_state.update(self._ep_info)
                sr.global_state["heartbeat_period"] = self.heartbeat_period
                sr.global_state["active"] = active
                msg: bytes = pack(sr)
                f = heartbeat_publisher.publish(msg)
                f.add_done_callback(_done_cb)

            except Exception as e:
                # Publishing didn't work -- quiesce and see if a simple restart
                # fixes the issue.
                self._quiesce_event.set()
                log.exception("Heartbeat failed; quiesce event set")
                f = Future()
                f.set_exception(e)
            return f

        stored_processor_thread = threading.Thread(
            target=process_stored_results, daemon=True, name="Stored Result Handler"
        )
        task_processor_thread = threading.Thread(
            target=process_pending_tasks, daemon=True, name="Pending Task Handler"
        )
        stored_processor_thread.start()
        task_processor_thread.start()

        if hasattr(engine, "executor_exception"):

            def engine_bad_state_watcher():
                while not self._quiesce_event.wait(0.5):
                    if engine.executor_exception:
                        self.stop()
                        log.exception(engine.executor_exception)

            threading.Thread(
                target=engine_bad_state_watcher, daemon=True, name="Bad State Watcher"
            ).start()

        connection_stable_hearbeats = 0
        last_t, last_r = 0, 0

        soft_idle_limit = max(0, self.config.idle_heartbeats_soft)
        hard_idle_limit = max(soft_idle_limit + 1, self.config.idle_heartbeats_hard)
        soft_idle_heartbeats = 0  # "happy path" idle timeout
        hard_idle_heartbeats = 0  # catch-all idle timeout

        live_proc_title = setproctitle.getproctitle()
        log.debug("_main_loop entered running state")
        heartbeat()
        while not self._quiesce_event.wait(self.heartbeat_period):
            with hb_lock:
                num_t = num_tasks_received
                num_r = num_results_forwarded
                diff_t, diff_r = num_t - last_t, num_r - last_r
                log.debug(
                    "Heartbeat.  Approximate Tasks received and Results forwarded"
                    " since last heartbeat: %s (T), %s (R)",
                    diff_t,
                    diff_r,
                )
                last_t, last_r = num_t, num_r

                # only reset come 2 heartbeats and still alive
                if self._reconnect_fail_counter:
                    connection_stable_hearbeats += 1
                    if connection_stable_hearbeats > 1:
                        log.info("Connection stable for 2 heartbeats; reset fail count")
                        self._reconnect_fail_counter = 0

                heartbeat()
                if not soft_idle_limit:
                    # idle timeout not enabled; "always on"
                    continue

                if diff_t or diff_r:
                    # a task moved; reset idle heartbeat counter
                    if soft_idle_heartbeats or hard_idle_heartbeats:
                        log.info(
                            "Moved to active state (due to tasks processed since"
                            " last heartbeat)."
                        )
                    setproctitle.setproctitle(live_proc_title)
                    soft_idle_heartbeats = 0
                    hard_idle_heartbeats = 0
                    continue

                # only start "timer" if we've at least done *some* work
                hard_idle_heartbeats += 1
                if (num_t or num_r) and num_r >= num_t:
                    # similar to above, only start "timer" if *idle* ... but
                    # note that given self.result_store, it's possible to
                    # have forwarded more results than tasks received.
                    soft_idle_heartbeats += 1
                    shutdown_s = soft_idle_limit - soft_idle_heartbeats
                    shutdown_s *= self.heartbeat_period

                    if soft_idle_heartbeats == 1:
                        log.info(
                            "In idle state (due to no task or result movement);"
                            f" shut down in {shutdown_s:,}s.  (idle_heartbeats_soft)"
                        )
                    idle_proc_title = "[idle; shut down in {:,}s] {}"
                    setproctitle.setproctitle(
                        idle_proc_title.format(shutdown_s, live_proc_title)
                    )

                    if soft_idle_heartbeats >= soft_idle_limit:
                        log.info("Idle heartbeats reached.  Shutting down.")
                        self.stop()

                elif hard_idle_heartbeats > hard_idle_limit:
                    log.warning("Shutting down due to idle heartbeats HARD limit.")
                    self.stop()

                elif hard_idle_heartbeats > soft_idle_limit:
                    # Reminder: this branch only hit if EP started and no tasks
                    # or results have moved.  If *any* movement occurs, this branch
                    # won't get executed.
                    shutdown_s = hard_idle_limit - hard_idle_heartbeats
                    shutdown_s *= self.heartbeat_period
                    if hard_idle_heartbeats == soft_idle_limit + 1:
                        # only log the first time; no sense in filling logs
                        log.info(
                            "Possibly idle -- no task or result movement.  Will"
                            f" shut down in {shutdown_s:,}s.  (idle_heartbeats_hard)"
                        )
                    idle_proc_title = "[possibly idle; shut down in {:,}s] {}"
                    setproctitle.setproctitle(
                        idle_proc_title.format(shutdown_s, live_proc_title)
                    )

        self.pending_task_queue.put(None)
        task_processor_thread.join()
        stored_processor_thread.join()

        # let higher-level error handling take over if the following excepts
        try:
            heartbeat(active=False).result(timeout=5)
        except concurrent.futures.TimeoutError:
            log.warning(
                "Unable to send final heartbeat (timeout sending); ignoring for quiesce"
            )

        task_q_subscriber.stop()
        results_publisher.stop(block=False)
        heartbeat_publisher.stop(block=False)

        log.debug("_main_loop exits")
