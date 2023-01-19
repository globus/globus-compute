from __future__ import annotations

import concurrent.futures
import logging
import os
import queue
import random
import sys
import threading
import time
import typing as t
import uuid
import warnings

if sys.version_info >= (3, 8):
    from concurrent.futures import InvalidStateError
else:

    class InvalidStateError(Exception):
        pass


import pika
from funcx_common import messagepack
from funcx_common.messagepack.message_types import Result

from funcx.errors import FuncxTaskExecutionFailed
from funcx.sdk.asynchronous.funcx_future import FuncXFuture
from funcx.sdk.client import FuncXClient
from funcx.sdk.utils import chunk_by

log = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    import pika.exceptions
    from pika.channel import Channel
    from pika.frame import Method
    from pika.spec import Basic, BasicProperties


class TaskSubmissionInfo:
    def __init__(
        self,
        *,
        task_num: int,
        function_id: str,
        endpoint_id: str,
        args: tuple,
        kwargs: dict,
    ):
        self.task_num = task_num
        self.function_id = function_id
        self.endpoint_id = endpoint_id
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return (
            "TaskSubmissionInfo("
            f"task_num={self.task_num}, "
            f"function_id='{self.function_id}', "
            f"endpoint_id='{self.endpoint_id}', "
            "args=..., kwargs=...)"
        )


class AtomicController:
    """This is used to synchronize between the FuncXExecutor which starts
    WebSocketPollingTasks and the WebSocketPollingTask which closes itself when there
    are 0 tasks.
    """

    def __init__(self, start_callback, stop_callback):
        self._value = 0
        self._lock = threading.Lock()
        self.start_callback = start_callback
        self.stop_callback = stop_callback

    def reset(self):
        """Reset the counter to 0; this method does not call callbacks"""
        with self._lock:
            self._value = 0

    def increment(self, val: int = 1):
        with self._lock:
            if self._value == 0:
                self.start_callback()
            self._value += val

    def decrement(self):
        with self._lock:
            self._value -= 1
            if self._value == 0:
                self.stop_callback()
            return self._value

    def value(self):
        with self._lock:
            return self._value

    def __repr__(self):
        return f"AtomicController value:{self._value}"


class FuncXExecutor(concurrent.futures.Executor):
    """
    Extend Python's |Executor|_ base class for funcX's purposes.

    .. |Executor| replace:: ``FuncXExecutor``
    .. _Executor: https://docs.python.org/3/library/concurrent.futures.html#executor-objects
    """  # noqa

    def __init__(
        self,
        endpoint_id: str | None = None,
        container_id: str | None = None,
        funcx_client: FuncXClient | None = None,
        task_group_id: str | None = None,
        label: str = "",
        batch_size: int = 128,
        **kwargs,
    ):
        """
        :param endpoint_id: id of the endpoint to which to submit tasks
        :param container_id: id of the container in which to execute tasks
        :param funcx_client: instance of FuncXClient to be used by the
            executor.  If not provided, the executor will instantiate one with default
            arguments.
        :param task_group_id: The Task Group to which to associate tasks.  If not set,
            one will be instantiated.
        :param label: a label to name the executor; mainly utilized for
            logging and advanced needs with multiple executors.
        :param batch_size: the maximum number of tasks to coalesce before
            sending upstream [min: 1, default: 128]
        :param batch_interval: [DEPRECATED; unused] number of seconds to coalesce tasks
            before submitting upstream
        :param batch_enabled: [DEPRECATED; unused] whether to batch results
        """
        deprecated_kwargs = {"batch_interval", "batch_enabled"}
        for key in kwargs:
            if key in deprecated_kwargs:
                warnings.warn(
                    f"`{key}` is not utilized and will be removed in a future release",
                    DeprecationWarning,
                )
                continue
            msg = f"'{key}' is an invalid argument for {self.__class__.__name__}"
            raise TypeError(msg)

        if not funcx_client:
            funcx_client = FuncXClient()
        self.funcx_client = funcx_client

        self.endpoint_id = endpoint_id
        self.container_id = container_id
        self.label = label
        self.batch_size = max(1, batch_size)

        self.task_count_submitted = 0
        self._task_counter: int = 0
        self._task_group_id: str = task_group_id or str(uuid.uuid4())
        self._tasks_to_send: queue.Queue[
            tuple[FuncXFuture, TaskSubmissionInfo] | tuple[None, None]
        ] = queue.Queue()
        self._function_registry: dict[t.Callable, str] = {}

        self._stopped = False
        self._stopped_in_error = False
        self._shutdown_lock = threading.RLock()
        self._result_watcher: _ResultWatcher | None = None

        log.debug("%s: initiated on thread: %s", self, threading.get_ident())
        self._task_submitter = threading.Thread(target=self._task_submitter_impl)
        self._task_submitter.start()

    def __repr__(self) -> str:
        name = self.__class__.__name__
        label = self.label and f"{self.label}, " or ""
        return f"{name}<{label}{self.batch_size}, ep_id:{self.endpoint_id}>"

    @property
    def task_group_id(self) -> str:
        """
        The Task Group with which this instance is currently associated.  New tasks will
        be sent to this Task Group upstream, and the result listener will only listen
        for results for this group.

        Must be a string.  Set by simple assignment::

            fxe = FuncXExecutor(endpoint_id="...")
            fxe.task_group_id = "Some-stored-id"

        This is typically used when reattaching to a previously initiated set of tasks.
        See `reload_tasks()`_ for more information.

        [default: ``None``, which translates to the FuncXClient task group id]
        """
        return self._task_group_id

    @task_group_id.setter
    def task_group_id(self, task_group_id: str):
        self._task_group_id = task_group_id

    def register_function(
        self, fn: t.Callable, function_id: str | None = None, **func_register_kwargs
    ) -> str:
        """
        Register a task function with this Executor's cache.

        All function execution submissions (i.e., ``.submit()``) communicate which
        pre-registered function to execute on the endpoint by the function's
        identifier, the ``function_id``.  This method makes the appropriate API
        call to the funcX web services to first register the task function, and
        then stores the returned ``function_id`` in the Executor's cache.

        In the standard workflow, ``.submit()`` will automatically handle invoking
        this method, so the common use-case will not need to use this method.
        However, some advanced use-cases may need to fine-tune the registration
        of a function and so may manually set the registration arguments via this
        method.

        If a function has already been registered (perhaps in a previous
        iteration), the upstream API call may be avoided by specifying the known
        ``function_id``.

        If a function already exists in the Executor's cache, this method will
        raise a ValueError to help track down the errant double registration
        attempt.

        :param fn: function to be registered for remote execution
        :param function_id: if specified, associate the ``function_id`` to the
            ``fn`` immediately, short-circuiting the upstream registration call.
        :param func_register_kwargs: all other keyword arguments are passed to
            the ``FuncXClient.register_function()``.
        :returns: the function's ``function_id`` string, as returned by
            registration upstream
        """
        if self._stopped:
            err_fmt = "%s is shutdown; refusing to register function"
            raise RuntimeError(err_fmt % repr(self))

        if fn in self._function_registry:
            msg = f"Function already registered as function id: {function_id}"
            log.error(msg)
            self.shutdown(wait=False, cancel_futures=True)
            raise ValueError(msg)

        if function_id:
            self._function_registry[fn] = function_id
            return function_id

        log.debug("Function not registered. Registering: %s", fn)
        func_register_kwargs.pop("function", None)
        reg_kwargs = {"function_name": fn.__name__}
        reg_kwargs.update(func_register_kwargs)
        try:
            func_reg_id = self.funcx_client.register_function(fn, **reg_kwargs)
        except Exception:
            log.error(f"Unable to register function: {fn.__name__}")
            self.shutdown(wait=False, cancel_futures=True)
            raise
        self._function_registry[fn] = func_reg_id
        log.debug("Function registered with id: %s", func_reg_id)
        return func_reg_id

    def submit(self, fn, *args, **kwargs):
        """
        Submit a function to be executed on the Executor's specified endpoint
        with the given arguments.

        Schedules the callable to be executed as ``fn(*args, **kwargs)`` and
        returns a FuncXFuture instance representing the execution of the
        callable.

        Example use::

            >>> def add(a: int, b: int) -> int: return a + b
            >>> fxe = FuncXExecutor(endpoint_id="some-ep-id")
            >>> fut = fxe.submit(add, 1, 2)
            >>> fut.result()    # wait (block) until result is received from remote
            3

        :param fn: Python function to execute on endpoint
        :param args: positional arguments (if any) as required to execute
            the function
        :param kwargs: keyword arguments (if any) as required to execute
            the function
        :returns: a future object that will receive a ``.task_id`` when the
            funcX Web Service acknowledges receipt, and eventually will have
            a ``.result()`` when the funcX web services receive and stream it.
        """
        if self._stopped:
            err_fmt = "%s is shutdown; no new functions may be executed"
            raise RuntimeError(err_fmt % repr(self))

        if fn not in self._function_registry:
            self.register_function(fn)

        fn_id = self._function_registry[fn]
        return self.submit_to_registered_function(
            function_id=fn_id, args=args, kwargs=kwargs
        )

    def submit_to_registered_function(
        self,
        function_id: str,
        args: tuple | None = None,
        kwargs: dict | None = None,
    ):
        """
        Request an execution of an already registered function.

        This method supports use of public functions with the FuncXExecutor, or
        knowledge of an already registered function.  An example use might be::

            # pre_registration.py
            from funcx import FuncXExecutor

            def some_processor(*args, **kwargs):
                # ... function logic ...
                return ["some", "result"]

            fxe = FuncXExecutor()
            fn_id = fxe.register_function(some_processor)
            print(f"Function registered successfully.\\nFunction ID: {fn_id}")

            # Example output:
            #
            # Function registered successfully.
            # Function ID: c407ae80-b31f-447a-9fa6-124098492057

        In this case, the function would be privately registered to you, but note that
        the function id is just a string.  One could substitute for a publicly
        available function.  For instance, ``b0a5d1a0-2b22-4381-b899-ba73321e41e0`` is
        a "well-known" uuid for the "Hello, World!" function (same as the example in
        the FuncX tutorial), which is publicly available::

            from funcx import FuncXExecutor

            fn_id = "b0a5d1a0-2b22-4381-b899-ba73321e41e0"  # public; "Hello World"
            with FuncXExecutor(endpoint_id="your-endpoint-id") as fxe:
                futs = [
                    fxe.submit_to_registered_function(function_id=fn_id)
                    for i in range(5)
                ]

            for f in futs:
                print(f.result())

        :param function_id: identifier (str) of registered Python function
        :param args: positional arguments (if any) as required to execute
            the function
        :param kwargs: keyword arguments (if any) as required to execute
            the function
        :returns: a future object that (eventually) will have a ``.result()``
            when the funcX web services receive and stream it.
        """
        if self._stopped:
            err_fmt = "%s is shutdown; no new functions may be executed"
            raise RuntimeError(err_fmt % repr(self))

        if not self.endpoint_id:
            msg = (
                "No endpoint_id set.  Did you forget to set it at construction?\n"
                "  Hint:\n\n"
                "    fxe = FuncXExecutor(endpoint_id=<ep_id>)\n"
                "    fxe.endpoint_id = <ep_id>    # alternative"
            )
            self.shutdown(wait=False, cancel_futures=True)
            raise ValueError(msg)

        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        self._task_counter += 1

        task = TaskSubmissionInfo(
            task_num=self._task_counter,  # unnecessary; maybe useful for debugging?
            function_id=function_id,
            endpoint_id=self.endpoint_id,
            args=args,
            kwargs=kwargs,
        )

        fut = FuncXFuture()
        self._tasks_to_send.put((fut, task))
        return fut

    def map(self, fn: t.Callable, *iterables, timeout=None, chunksize=1) -> t.Iterator:
        """
        FuncX does not currently implement the `.map()`_ method of the `Executor
        interface`_.  In a naive implementation, this method would merely be
        syntactic sugar for bulk use of the ``.submit()`` method.  For example::

            def map(fxexec, fn, *fn_args_kwargs):
                return [fxexec.submit(fn, *a, **kw) for a, kw in fn_args_kwargs]

        This naive implementation ignores a number of potential optimizations, so
        we have decided to look at this at a future date if there is interest.

        .. _.map(): https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor.map
        .. _Executor interface: https://docs.python.org/3/library/concurrent.futures.html#executor-objects

        Raises
        ------
        NotImplementedError
          always raised
        """  # noqa
        raise NotImplementedError()

    def reload_tasks(self) -> t.Iterable[FuncXFuture]:
        """
        .. _reload_tasks():

        Load the set of tasks associated with this Executor's Task Group from the
        web services and return a list of futures, one for each task.  This is
        nominally intended to "reattach" to a previously initiated session, based on
        the Task Group ID.

        :returns: An iterable of futures.
        :raises ValueError: if the server response is incorrect or invalid
        :raises KeyError: the server did not return an expected response
        :raises various: the usual (unhandled) request errors (e.g., no connection;
            invalid authorization)

        Notes
        -----
        Any previous futures received from this executor will be cancelled.
        """  # noqa
        # step 1: cleanup!
        if self._result_watcher:
            self._result_watcher.shutdown(wait=False, cancel_futures=True)
            self._result_watcher = None

        task_group_id = self.task_group_id  # snapshot
        # step 2: from server, acquire list of related task ids and make futures
        r = self.funcx_client.web_client.get_taskgroup_tasks(task_group_id)
        if r["taskgroup_id"] != task_group_id:
            msg = (
                "Server did not respond with requested TaskGroup Tasks.  "
                f"(Requested tasks for {task_group_id} but received "
                f"tasks for {r['taskgroup_id']}"
            )
            raise ValueError(msg)

        # step 3: create the associated set of futures
        task_ids: list[str] = [task["id"] for task in r.get("tasks", [])]
        futures: list[FuncXFuture] = []

        if task_ids:
            # Complete the futures that already have results.
            pending: list[FuncXFuture] = []
            deserialize = self.funcx_client.fx_serializer.deserialize
            chunk_size = 1024
            num_chunks = len(task_ids) // chunk_size + 1
            for chunk_no, id_chunk in enumerate(
                chunk_by(task_ids, chunk_size), start=1
            ):
                if num_chunks > 1:
                    log.debug(
                        "Large task group (%s tasks; %s chunks); retrieving chunk %s"
                        " (%s tasks)",
                        len(task_ids),
                        num_chunks,
                        chunk_no,
                        len(id_chunk),
                    )

                res = self.funcx_client.web_client.get_batch_status(id_chunk)
                for task_id, task in res.data.get("results", {}).items():
                    fut = FuncXFuture(task_id)
                    futures.append(fut)
                    completed_t = task.get("completion_t")
                    if not completed_t:
                        pending.append(fut)
                    else:
                        try:
                            if task.get("status") == "success":
                                fut.set_result(deserialize(task["result"]))
                            else:
                                exc = FuncxTaskExecutionFailed(
                                    task["exception"], completed_t
                                )
                                fut.set_exception(exc)
                        except Exception as exc:
                            funcx_err = FuncxTaskExecutionFailed(
                                "Failed to set result or exception"
                            )
                            funcx_err.__cause__ = exc
                            fut.set_exception(funcx_err)

            if pending:
                self._result_watcher = _ResultWatcher(self)
                self._result_watcher.watch_for_task_results(pending)
                self._result_watcher.start()
        else:
            log.warning(f"Received no tasks for Task Group ID: {task_group_id}")

        # step 5: the goods for the consumer
        return futures

    def shutdown(self, wait=True, *, cancel_futures=False):
        thread_id = threading.get_ident()
        log.debug("%s: initiating shutdown (thread: %s)", self, thread_id)
        if self._task_submitter.is_alive():
            self._tasks_to_send.put((None, None))
            if wait:
                self._tasks_to_send.join()

        with self._shutdown_lock:
            self._stopped = True
            if self._result_watcher:
                self._result_watcher.shutdown(wait=wait, cancel_futures=cancel_futures)
                self._result_watcher = None

        if thread_id != self._task_submitter.ident and self._stopped_in_error:
            # In an unhappy path scenario, there's the potential for multiple
            # tracebacks, which means that the tracebacks will likely be
            # interwoven in the logs.  Attempt to make debugging easier for
            # that scenario by adding a slight delay on the *main* thread.
            time.sleep(0.1)
        log.debug("%s: shutdown complete (thread: %s)", self, thread_id)

    def _task_submitter_impl(self) -> None:
        """
        Coalesce tasks from the interthread queue (``_tasks_to_send``), up to
        ``self.batch_size``, submit them, and then send the futures to the
        ResultWatcher.

        The main job of this method is to loop forever, forwarding task
        requests upstream and the associated futures to the ResultWatcher.

        This thread stops when it receives a poison-pill of ``(None, None)``
        in the queue.  (See ``shutdown()``.)
        """
        log.debug(
            "%s: task submission thread started (%s)", self, threading.get_ident()
        )
        to_send = self._tasks_to_send  # cache lookup
        futs: list[FuncXFuture] = []  # for mypy/the exception branch
        try:
            fut: FuncXFuture | None = FuncXFuture()  # just start the loop; please
            while fut is not None:
                futs = []
                tasks: list[TaskSubmissionInfo] = []
                task_count = 0
                try:
                    fut, task = to_send.get()  # Block; wait for first result ...
                    task_count += 1
                    bs = max(1, self.batch_size)  # May have changed while waiting
                    while task is not None:
                        assert fut is not None  # Come on mypy; contextually clear!
                        tasks.append(task)
                        futs.append(fut)
                        if not (len(tasks) < bs):
                            break
                        fut, task = to_send.get(block=False)  # ... don't block again
                        task_count += 1
                except queue.Empty:
                    pass

                if not tasks:
                    continue

                log.info(f"Submitting tasks to funcX: {len(tasks)}")
                self._submit_tasks(futs, tasks)

                with self._shutdown_lock:
                    if self._stopped:
                        continue

                    if not (self._result_watcher and self._result_watcher.is_alive()):
                        # Don't initialize the result watcher unless at least
                        # one batch has been sent
                        self._result_watcher = _ResultWatcher(self)
                        self._result_watcher.start()
                    try:
                        self._result_watcher.watch_for_task_results(futs)
                    except self._result_watcher.__class__.ShuttingDownError:
                        log.debug("Waiting for previous ResultWatcher to shutdown")
                        self._result_watcher.join()
                        self._result_watcher = _ResultWatcher(self)
                        self._result_watcher.start()
                        self._result_watcher.watch_for_task_results(futs)

                # important to clear futures; else a legitimately early-shutdown
                # request (e.g., __exit__()) can cancel these (finally block,
                # below) before the result comes back, even though _result_watcher
                # is already watching them.
                futs.clear()

                while task_count:
                    task_count -= 1
                    to_send.task_done()

        except Exception as exc:
            self._stopped = True
            self._stopped_in_error = True
            log.debug(
                "%s: task submission thread encountered error ([%s] %s)",
                self,
                exc.__class__.__name__,
                exc,
            )

            if self._shutdown_lock.acquire(blocking=False):
                self.shutdown(wait=False, cancel_futures=True)
                self._shutdown_lock.release()

            log.debug("%s: task submission thread dies", self)
            raise
        finally:
            if sys.exc_info() != (None, None, None):
                time.sleep(0.1)  # give any in-flight Futures a chance to be .put() ...
            while not self._tasks_to_send.empty():
                fut, _task = self._tasks_to_send.get()
                if fut:
                    futs.append(fut)

            for fut in futs:
                fut.cancel()
                fut.set_running_or_notify_cancel()
            try:
                while True:
                    self._tasks_to_send.task_done()
            except ValueError:
                pass
            log.debug("%s: task submission thread complete", self)

    def _submit_tasks(self, futs: list[FuncXFuture], tasks: list[TaskSubmissionInfo]):
        """
        Submit a batch of tasks to the webservice, destined for self.endpoint_id.
        Upon success, update the futures with their associated task_id.

        :param futs: a list of FuncXFutures; will have their task_id attribute
            set when function completes successfully.
        :param tasks: a list of tasks to submit upstream in a batch.
        """
        batch = self.funcx_client.create_batch(
            task_group_id=self.task_group_id,
            create_websocket_queue=True,
        )
        for task in tasks:
            batch.add(task.function_id, task.endpoint_id, task.args, task.kwargs)
            log.debug("Added task to funcX batch: %s", task)

        try:
            batch_tasks = self.funcx_client.batch_run(batch)
        except Exception:
            log.error(f"Error submitting {len(tasks)} tasks to funcX")
            raise

        self.task_count_submitted += len(batch_tasks)
        log.debug(
            "Batch submitted to task_group: %s - %s",
            self.task_group_id,
            self.task_count_submitted,
        )

        for fut, task_uuid in zip(futs, batch_tasks):
            fut.task_id = task_uuid


class _ResultWatcher(threading.Thread):
    """
    _ResultWatcher is an internal SDK class meant for consumption by the
    FuncXExecutor.  It is a standard async AMQP consumer implementation
    using the Pika library that matches futures from the Executor against
    results received from the funcX hosted services.

    Expected usage::

        rw = _ResultWatcher(self)  # assert isinstance(self, FuncXExecutor)
        rw.start()

        # rw is its own thread; it will use the FuncXClient attached to the
        # FuncXExecutor to acquire AMQP credentials, and then will open a
        # connection to the AMQP service.

        rw.watch_for_task_results(some_list_of_futures)

    When there are no more open futures and no more results, the thread
    will opportunistically shutdown; the caller must handle this scenario
    if new futures arrive, and create a new _ResultWatcher instance.

    :param funcx_executor: A FuncXExecutor instance
    :param poll_period_s: [default: 0.5] how frequently to check for and
        handle events.  For example, if the thread should stop due to user
        request or if there are results to match.
    :param connect_attempt_limit: [default: 3] how many times to attempt
        connecting to the AMQP server before bailing.
    :param channel_close_window_s: [default: 10] how large a window to
        tally channel open events.
    :param channel_close_window_limit: [default: 3] how many reopen
        attempts to allow within the tally window before concluding
        there is an external error and shutting down the watcher.
    """

    class ShuttingDownError(Exception):
        pass

    def __init__(
        self,
        funcx_executor: FuncXExecutor,
        poll_period_s=0.5,
        connect_attempt_limit=5,
        channel_close_window_s=10,
        channel_close_window_limit=3,
    ):
        super().__init__()
        self.funcx_executor = funcx_executor
        self._to_ack: list[int] = []  # outstanding amqp messages not-yet-acked
        self._time_to_check_results = threading.Event()
        self._new_futures_lock = threading.Lock()

        self._connection: pika.SelectConnection | None = None
        self._channel: Channel | None = None
        self._consumer_tag: str | None = None

        self._queue_prefix = ""

        self._open_futures: dict[str, FuncXFuture] = {}
        self._received_results: dict[str, tuple[BasicProperties, Result]] = {}

        self._open_futures_empty = threading.Event()
        self._open_futures_empty.set()
        self._time_to_stop = False

        # how often to check for work; every `poll_period_s`, the `_event_watcher`
        # method will invoke to match results to tasks, ack upstream, and see if
        # it's time to shut down the connection and thread.
        self.poll_period_s = poll_period_s
        self._connection_tries = 0  # count of connection events; reset on success

        # how many times to attempt connection before giving up and shutting
        # down the thread
        self.connect_attempt_limit = connect_attempt_limit

        self._thread_id = 0  # 0 == the parent process; will be set by .run()
        self._cancellation_reason: Exception | None = None
        self._closed = False  # precursor flag until the thread stops completely

        # list of times that channel was last closed
        self._channel_closes: list[float] = []

        # how long a time frame to keep previous channel close times
        self.channel_close_window_s = channel_close_window_s

        # how many times allowed to retry opening a channel in the above time
        # window before giving up and shutting down the thread
        self.channel_close_window_limit = channel_close_window_limit

    def __repr__(self):
        return "{}<{}; pid={}; fut={:,d}; res={:,d}; qp={}>".format(
            self.__class__.__name__,
            "✓" if self._consumer_tag else "✗",
            os.getpid(),
            len(self._open_futures),
            len(self._received_results),
            self._queue_prefix if self._queue_prefix else "-",
        )

    def run(self):
        log.debug("%r AMQP thread begins", self)
        self._thread_id = threading.get_ident()

        while self._connection_tries < self.connect_attempt_limit and not (
            self._time_to_stop or self._cancellation_reason
        ):
            if self._connection or self._connection_tries:
                wait_for = random.uniform(0.5, 10)
                msg = f"%r AMQP reconnecting in {wait_for:.1f}s."
                log.debug(msg, self)
                if self._connection_tries == self.connect_attempt_limit - 1:
                    log.warning(f"{msg}  (final attempt)", self)
                time.sleep(wait_for)

            self._connection_tries += 1
            try:
                log.debug(
                    "%r Opening connection to AMQP service.  Attempt: %s (of %s)",
                    self,
                    self._connection_tries,
                    self.connect_attempt_limit,
                )
                self._connection = self._connect()
                self._event_watcher()
                self._connection.ioloop.start()  # Reminder: blocks

            except Exception:
                log.exception("%r Unhandled error; shutting down", self)

        with self._new_futures_lock:
            if self._open_futures:
                if not self._time_to_stop or self._cancellation_reason:
                    # We're not shutting down intentionally; cancel outstanding
                    # futures so consumer learns right away of problem.
                    self._time_to_stop = True  # prevent new futures from arriving
                    log.warning("Cancelling all known futures")
                    if not self._cancellation_reason:
                        self._cancellation_reason = RuntimeError(
                            "SDK thread quit unexpectedly."
                        )

                    while self._open_futures:
                        _, fut = self._open_futures.popitem()
                        fut.set_exception(self._cancellation_reason)
                        log.debug("Cancelled: %s", fut.task_id)
                    self._open_futures_empty.set()
        log.debug("%r AMQP thread complete.", self)

    def shutdown(self, wait=True, *, cancel_futures=False):
        if not self.is_alive():
            return

        self._closed = True  # No more futures will be accepted

        if threading.get_ident() == self._thread_id:
            self._stop_ioloop()

        else:
            # External thread called this method
            if cancel_futures:
                with self._new_futures_lock:
                    while self._open_futures:
                        _, fut = self._open_futures.popitem()
                        fut.cancel()
                        fut.set_running_or_notify_cancel()
                    self._open_futures_empty.set()

            # N.B. Per Python Executor documentation, the whole program can't
            # exit() until all futures are complete, regardless of the `wait`
            # argument.  Thus, the _stopper_thread is *not* daemonized, and
            # the stopping of the processing thread happens *after* there
            # are no more pending futures.
            # See: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor.shutdown # noqa
            def _stopper_thread():
                if self._open_futures:
                    self._open_futures_empty.wait()
                self._time_to_stop = True
                self.join()

            join_thread = threading.Thread(target=_stopper_thread)
            join_thread.start()
            if wait:
                join_thread.join()

    def watch_for_task_results(self, futures: list[FuncXFuture]) -> int:
        """
        Add list of FuncXFutures to internal watch list.

        Updates the thread's dictionary of futures that will be resolved when
        upstream sends associated results.  The internal dictionary is keyed
        on the FuncXFuture.task_id attribute, but this method does not verify
        that it is set -- it is up to the caller to ensure the future is
        initialized fully.  In particular, if a task_id is not set, it will
        not be added to the watch list.

        ``watch_for_task_results()`` may raise ``_ResultWatcher.ShuttingDownError``
        if, when called, the object is shutting down.  This may happen for a
        few reasons (e.g., connection errors, channel errors) -- it is the
        caller's responsibility to handle this error.

        :returns: number of tasks added to watch list
        :raises ShuttingDownError: if called while this object is shutting down
        """
        with self._new_futures_lock:
            if self._closed:
                msg = "Unable to watch futures: %s is shutting down."
                raise self.__class__.ShuttingDownError(msg % self.__class__.__name__)

            to_watch = {
                f.task_id: f
                for f in futures
                if f.task_id and f.task_id not in self._open_futures
            }
            self._open_futures.update(to_watch)

            if self._open_futures:  # futures as an empty list is acceptable
                self._open_futures_empty.clear()
                if self._received_results:
                    # no sense is setting the event if there are not yet
                    # any results
                    self._time_to_check_results.set()
        return len(to_watch)

    def _match_results_to_futures(self):
        """
        Match the internal ``_received_results`` and ``_open_futures`` on their
        keys, and complete the associated futures.  All matching items will,
        after processing, be forgotten (i.e., ``.pop()``).

        This method will set the _open_futures_empty event if there are no open
        futures *at the time of processing*.
        """
        deserialize = self.funcx_executor.funcx_client.fx_serializer.deserialize
        with self._new_futures_lock:
            futures_to_complete = [
                self._open_futures.pop(tid)
                for tid in self._open_futures.keys() & self._received_results.keys()
            ]
            if not self._open_futures:
                self._open_futures_empty.set()

        for fut in futures_to_complete:
            props, res = self._received_results.pop(fut.task_id)

            if res.is_error:
                fut.set_exception(
                    FuncxTaskExecutionFailed(res.data, str(props.timestamp or 0))
                )
            else:
                try:
                    fut.set_result(deserialize(res.data))
                except InvalidStateError as err:
                    log.error(
                        f"Unable to set future state ({err}) for task: {fut.task_id}"
                    )
                except Exception as exc:
                    task_exc = Exception(
                        f"Malformed or unexpected data structure. Data: {res.data}",
                    )
                    task_exc.__cause__ = exc
                    fut.set_exception(task_exc)

    def _event_watcher(self):
        """
        Check, every iteration, for the following events and act accordingly::

        - If it's time to stop the thread (``_time_to_stop``)
        - If there are any open messages to ACKnowledge to the AMQP service
        - If there are new results or futures to match (``_time_to_check_results``)

        Notably, if, at the end of processing, there are no open futures or
        results, then initiate a shutdown.

        The ``_event_watcher()` method is initiated before the eventloop starts
        (see ``.run()``), and rearms itself every iteration.
        """
        if self._time_to_stop:
            self.shutdown()
            log.debug("%r Shutdown complete", self)
            return

        try:
            if self._to_ack:
                self._to_ack.sort()  # no change in the happy path
                latest_msg_id = self._to_ack[-1]
                self._channel.basic_ack(latest_msg_id, multiple=True)
                self._to_ack.clear()
                log.debug("%r Acknowledged through message: %s", self, latest_msg_id)

            if self._time_to_check_results.is_set():
                self._time_to_check_results.clear()
                self._match_results_to_futures()
                if not (self._open_futures or self._received_results):
                    log.debug("%r Idle; no outstanding results or futures.", self)
        finally:
            self._connection.ioloop.call_later(self.poll_period_s, self._event_watcher)

    def _on_message(
        self,
        channel: Channel,
        basic_deliver: Basic.Deliver,
        props: BasicProperties,
        body: bytes,
    ):
        """
        Nominally, the kernel of this whole class -- called by the Pika library
        (ioloop) when a new Result message has arrived from upstream.  This
        method will attempt to unpack the bytes so as to minimally verify that
        the message is a valid Result object, and then store the unpacked
        object in self._received_results for later processing and set the
        ``_time_to_check_results`` event.

        If the received message is *not* a result, then immediately NACK the
        message to AMQP service -- the responsibility to handle invalid messages
        is not with this class.
        """
        msg_id: int = basic_deliver.delivery_tag
        log.debug("Received message %s: [%s] %s", msg_id, props, body)

        try:
            res = messagepack.unpack(body)
            if not isinstance(res, Result):
                raise TypeError(f"Non-Result object received ({type(res)})")

            self._received_results[str(res.task_id)] = (props, res)
            self._to_ack.append(msg_id)
            self._time_to_check_results.set()
        except Exception:
            # No sense in waiting for the RMQ default 30m timeout; let it know
            # *now* that this message failed.
            log.exception("Invalid message type queue put failed")
            channel.basic_nack(msg_id, requeue=True)

    def _stop_ioloop(self):
        """
        Gracefully stop the ioloop.

        In an effort play nice with upstream, attempt to follow the AMQP protocol
        by closing the channel and connections gracefully.  This method will
        rearm itself while the connection is still open, continually working
        toward eventually and gracefully stopping the connection, before finally
        stopping the ioloop.
        """
        if self._connection:
            self._connection.ioloop.call_later(0.1, self._stop_ioloop)
            if self._connection.is_open:
                if self._channel:
                    if self._channel.is_open:
                        self._channel.close()
                    elif self._channel.is_closed:
                        self._channel = None
                else:
                    self._connection.close()
            elif self._connection.is_closed:
                self._connection.ioloop.stop()
                self._connection = None

    def _connect(self) -> pika.SelectConnection:
        with self._new_futures_lock:
            res = self.funcx_executor.funcx_client.get_result_amqp_url()
            self._queue_prefix = res["queue_prefix"]
            connection_url = res["connection_url"]

            pika_params = pika.URLParameters(connection_url)
            return pika.SelectConnection(
                pika_params,
                on_close_callback=self._on_connection_closed,
                on_open_error_callback=self._on_open_failed,
                on_open_callback=self._on_connection_open,
            )

    def _on_open_failed(self, _mq_conn: pika.BaseConnection, exc: str | Exception):
        assert self._connection is not None, "Strictly called _by_ ioloop"
        count = f"[attempt {self._connection_tries} (of {self.connect_attempt_limit})]"
        pid = f"(pid: {os.getpid()})"
        exc_text = f"Failed to open connection - ({exc.__class__.__name__}) {exc}"
        msg = f"{count} {pid} {exc_text}"
        log.debug(msg)

        if not (self._connection_tries < self.connect_attempt_limit):
            log.warning(msg)
            if not isinstance(exc, Exception):
                exc = Exception(str(exc))
            self._cancellation_reason = exc
        self._connection.ioloop.stop()

    def _on_connection_closed(self, _mq_conn: pika.BaseConnection, exc: Exception):
        assert self._connection is not None, "Strictly called _by_ ioloop"
        log.debug("Connection closed: %s", exc)
        self._consumer_tag = None
        self._connection.ioloop.stop()

    def _on_connection_open(self, _mq_conn: pika.BaseConnection):
        log.debug("Connection established; creating channel")
        self._connection_tries = 0
        self._open_channel()

    def _open_channel(self):
        if self._connection.is_open:
            self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel: Channel):
        self._channel = channel

        channel.add_on_close_callback(self._on_channel_closed)
        channel.add_on_cancel_callback(self._on_consumer_cancelled)

        log.debug(
            "Channel %s opened (%s); begin consuming messages.",
            channel.channel_number,
            channel.connection.params,
        )
        self._start_consuming()

    def _on_channel_closed(self, channel: Channel, exc: Exception):
        if self._closed:
            return

        self._consumer_tag = None
        assert self._connection is not None, "Strictly called _by_ ioloop"
        # Doh!  Channel closed unexpectedly.
        now = time.monotonic()
        then = now - self.channel_close_window_s
        self._channel_closes = [cc for cc in self._channel_closes if cc > then]
        self._channel_closes.append(now)
        if len(self._channel_closes) < self.channel_close_window_limit:
            msg = f"Channel closed.  Reopening.\n  {channel}\n  ({exc})"
            log.debug(msg, exc_info=exc)
            log.warning(msg)
            self._connection.ioloop.call_later(1, self._open_channel)

        else:
            msg = (
                f"Unable to sustain channel after {len(self._channel_closes)} attempts"
                f" in {self.channel_close_window_limit} seconds. ({exc})"
            )
            log.error(msg)
            self._closed = True
            self._cancellation_reason = RuntimeError(msg)
            self.shutdown()

    def _start_consuming(self):
        self._consumer_tag = self._channel.basic_consume(
            queue=f"{self._queue_prefix}{self.funcx_executor.task_group_id}",
            on_message_callback=self._on_message,
        )

    def _on_consumer_cancelled(self, frame: Method[Basic.CancelOk]):
        log.info("Consumer cancelled remotely, shutting down: %r", frame)
        if self._channel:
            self._channel.close()
