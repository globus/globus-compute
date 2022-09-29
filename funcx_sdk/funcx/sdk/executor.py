from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import logging
import queue
import threading
import typing as t
import warnings

from funcx.sdk.asynchronous.funcx_future import FuncXFuture
from funcx.sdk.asynchronous.ws_polling_task import WebSocketPollingTask
from funcx.sdk.client import FuncXClient

log = logging.getLogger(__name__)


class TaskSubmissionInfo:
    def __init__(
        self,
        *,
        task_num: int,
        function_id: str,
        endpoint_id: str,
        args: t.Tuple[t.Any],
        kwargs: t.Dict[str, t.Any],
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
    The ``FuncXExecutor`` class, a subclass of `concurrent.futures.Executor`_, is the
    preferred approach to collecting results from the funcX Web Service.  Over
    polling (the historical approach) where the web service must be repeatedly
    queried for the status of tasks and results eventually collected in bulk, the
    ``FuncXExecutor`` class instantiates a WebSocket connection that streams results
    directly -- and immediately -- as they arrive at the server.  This is a far more
    efficient paradigm, simultaneously in terms of bytes over the wire, time spent
    waiting for results, and boilerplate code to check for results.

    An interaction might look like::

        from funcx import FuncXExecutor
        from funcx.sdk.executor import FuncXFuture

        fxexec = FuncXExecutor()
        ep_id = "<YOUR_ENDPOINT_UUID>"

        def example_funcx_kernel(num):
            import time
            time.sleep(num * random.random())  # simulate some processing
            return f"result, from task: {num}"

        futs: list[FuncXFuture] = [
            fxexec.submit(example_funcx_kernel, task_i, endpoint_id=ep_id)
            for task_i in range(1, 21)
        ]
        # FuncXFuture is a subclass of concurrent.futures.Future

        results, exceptions = [], []
        for f in concurrent.futures.as_completed(futs, timeout=30):
            # wait no more than 30s for all results
            try:
                results.append(f.result())
            except Exception as exc:
                exceptions.append((f.task_id, exc))

        print("Results received (unordered):\\n  ", "\\n  ".join(results))
        for task_id, exc in exceptions:
            print(f"  Exception received from task {task_id}: {exc}")

    Each future returned by ``.submit()`` is a handle to that particular task's result;
    that future will be completed by a background thread in the FuncXExecutor as soon
    as the server sends the result -- no polling, just an event-based interaction.

    .. _concurrent.futures.Executor: https://docs.python.org/3/library/concurrent.futures.html#executor-objects
    """  # noqa

    def __init__(
        self,
        funcx_client: FuncXClient | None = None,
        label: str = "FuncXExecutor",
        batch_enabled: bool = True,
        batch_size: int = 100,
        **kwargs,
    ):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        label : str
            Optional string label to name the executor.
            Default: 'FuncXExecutor'
        """

        if "batch_interval" in kwargs:
            warnings.warn(
                "`batch_interval` is deprecated; it will be removed in "
                "a future release",
                DeprecationWarning,
            )

        if funcx_client:
            self.funcx_client = funcx_client
        else:
            self.funcx_client = FuncXClient()

        self.label = label
        self.batch_enabled = batch_enabled
        self.batch_size = batch_size
        self.task_outgoing: queue.Queue[
            tuple[FuncXFuture, TaskSubmissionInfo] | tuple[None, None]
        ] = queue.Queue()

        self.task_count_submitted = 0
        self._task_counter: int = 0
        self._function_registry: t.Dict[t.Any, str] = {}
        self._function_future_map: t.Dict[str, FuncXFuture] = {}
        self._kill_event: t.Optional[threading.Event] = None
        self._task_submit_thread: t.Optional[threading.Thread] = None

        self.poller_thread = ExecutorPollerThread(
            self.funcx_client,
            self._function_future_map,
        )

        self._reset_poller()

        if self.batch_enabled:
            log.debug("Batch submission enabled.")
            self.start_batching_thread()

        atexit.register(self.shutdown)

    def _reset_poller(self):
        if self.poller_thread.is_running:
            self.poller_thread.shutdown()
        self.poller_thread.atomic_controller.reset()

        self._task_counter = 0
        while self._function_future_map:
            _, fut = self._function_future_map.popitem()
            if not fut.done():
                fut.cancel()

    @property
    def results_ws_uri(self) -> str:
        return self.funcx_client.results_ws_uri

    @property
    def task_group_id(self) -> str:
        return self.funcx_client.session_task_group_id

    def start_batching_thread(self):
        self._kill_event = threading.Event()
        # Start the task submission thread
        self._task_submit_thread = threading.Thread(
            target=self._submit_task_kernel,
            args=(self._kill_event,),
            name="FuncX-Submit-Thread",
        )
        self._task_submit_thread.daemon = True
        self._task_submit_thread.start()
        log.debug("Started task submit thread")

    def register_function(self, func: t.Callable, container_uuid=None):
        # Please note that this is a partial implementation, not all function
        # registration options are fleshed out here.
        log.debug(f"Function:{func} is not registered. Registering")
        try:
            function_id = self.funcx_client.register_function(
                func,
                function_name=func.__name__,
                container_uuid=container_uuid,
            )
        except Exception:
            log.error(f"Error in registering {func.__name__}")
            raise
        else:
            self._function_registry[func] = function_id
            log.debug(f"Function registered with id:{function_id}")

    def submit(self, function, *args, endpoint_id=None, container_uuid=None, **kwargs):
        """Initiate an invocation

        Parameters
        ----------
        function : Function/Callable
            Function / Callable to execute

        *args : Any
            Args as specified by the function signature

        endpoint_id : uuid str
            Endpoint UUID string. Required

        **kwargs : Any
            Arbitrary kwargs

        Returns
        -------
        future: FuncXFuture
           A future object, that will receive a ``.task_id`` when the funcX Web Service
           acknowledges receipt, and eventually will have a ``.result()`` when the Web
           Service streams it over the WebSocket.
        """

        if function not in self._function_registry:
            self.register_function(function)
        self._task_counter += 1

        assert endpoint_id is not None, "endpoint_id key-word argument must be set"

        task = TaskSubmissionInfo(
            task_num=self._task_counter,  # unnecessary; maybe useful for debugging?
            function_id=self._function_registry[function],
            endpoint_id=endpoint_id,
            args=args,
            kwargs=kwargs,
        )

        fut = FuncXFuture()

        if self.batch_enabled:
            self.task_outgoing.put((fut, task))
        else:
            self._submit_tasks([fut], [task])

        return fut

    def _submit_task_kernel(self, kill_event: threading.Event):
        """
        Fetch enqueued tasks task_outgoing queue and submit them to funcX in batches
        of up to self.batch_size.

        Parameters
        ==========
        kill_event : threading.Event
            Sentinel event; used to stop the thread and exit.
        """
        to_send = self.task_outgoing  # cache lookup
        while not kill_event.is_set():
            futs: list[FuncXFuture] = []
            tasks: list[TaskSubmissionInfo] = []
            try:
                fut, task = to_send.get()  # Block while waiting for first result ...
                while task is not None:
                    assert fut is not None  # Come on, mypy; this contextually clear
                    tasks.append(task)
                    futs.append(fut)
                    if not (len(tasks) < self.batch_size):
                        break
                    fut, task = to_send.get(block=False)  # ... don't block thereafter
            except queue.Empty:
                pass
            if tasks:
                log.info(f"Submitting tasks to funcX: {len(tasks)}")
                self._submit_tasks(futs, tasks)

        log.debug("Exiting")

    def _submit_tasks(self, futs: list[FuncXFuture], tasks: list[TaskSubmissionInfo]):
        """Submit a batch of tasks"""
        batch = self.funcx_client.create_batch(
            task_group_id=self.task_group_id,
            create_websocket_queue=True,
        )
        for task in tasks:
            batch.add(
                *task.args,
                **task.kwargs,
                endpoint_id=task.endpoint_id,
                function_id=task.function_id,
            )
            log.debug(f"Adding task {task} to funcX batch")
        try:
            batch_tasks = self.funcx_client.batch_run(batch)
            self.task_count_submitted += len(batch_tasks)
            log.debug(
                "Batch submitted to task_group: %s - %s",
                self.task_group_id,
                self.task_count_submitted,
            )
        except Exception:
            log.error(f"Error submitting {len(tasks)} tasks to funcX")
            raise
        else:
            for fut, task_uuid in zip(futs, batch_tasks):
                fut.task_id = task_uuid
                self._function_future_map[task_uuid] = fut
            self.poller_thread.atomic_controller.increment(val=len(batch_tasks))

    def reload_tasks(self) -> t.Iterable[FuncXFuture]:
        """
        Load the set of tasks associated with this Executor's Task Group (FuncXClient)
        from the server and return a set of futures, one for each task.  This is
        nominally intended to "reattach" to a previously initiated session, based on
        the Task Group ID.  An example use might be::

            import sys
            import typing as T
            from funcx import FuncXClient, FuncXExecutor
            from funcx.sdk.executor import FuncXFuture

            fxc_kwargs = {}
            if len(sys.argv) > 1:
                fxc_kwargs["task_group_id"] = sys.argv[1]

            def example_funcx_kernel(num):
                result = f"your funcx logic result, from task: {num}"
                return result

            fxclient = FuncXClient(**fxc_kwargs)
            fxexec = FuncXExecutor(fxclient)

            # Save the task_group_id somewhere.  Perhaps in a file, or less
            # robustly "as mere text" on your console:
            print("If this script dies, rehydrate futures with this "
                 f"Task Group ID: {fxexec.task_group_id}")

            futures: T.Iterable[FuncXFuture] = []
            results, exceptions = [], []
            if "task_group_id" in fxc_kwargs:
                print(f"Reloading tasks from Task Group ID: {fxexec.task_group_id}")
                futures = fxexec.reload_tasks()

                # Ask server once up-front if there are any known results before
                # waiting for each result in turn (below):
                task_ids = [f.task_id for f in futures]
                finished_tasks = set()
                for task_id, state in fxclient.get_batch_result(task_ids).items():
                    if not state["pending"]:
                        finished_tasks.add(task_id)
                        if state["status"] == "success":
                            results.append(state["result"])
                        else:
                            exceptions.append(state["exception"])
                futures = [f for f in futures if f.task_id not in finished_tasks]

            else:
                print("New session; creating FuncX tasks ...")
                ep_id = "<YOUR_ENDPOINT_UUID>"
                for i in range(1, 5):
                    futures.append(
                        fxexec.submit(example_funcx_kernel, endpoint_id=ep_id)
                    )

                # ... Right here, your script dies for [SILLY REASON;
                #           DID YOU LOSE POWER?] ...

            # Get results:
            for f in futures:
                try:
                    results.append(f.result(timeout=10))
                except Exception as exc:
                    exceptions.append(exc)

        Returns
        -------
        An iterable of futures.

        Raises
        ------
        ValueError: if the server response is incorrect
        KeyError: if the server did not return an expected response
        various: the usual (unhandled) request errors (e.g., no connection; invalid authorization)

        Notes
        -----
        Any previous futures received from this executor will be cancelled.
        """  # noqa

        # step 1: cleanup!  Turn off poller_thread, clear _function_future_map
        self._reset_poller()

        # step 2: from server, acquire list of related task ids and make futures
        r = self.funcx_client.web_client.get_taskgroup_tasks(self.task_group_id)
        if r["taskgroup_id"] != self.task_group_id:
            msg = (
                "Server did not respond with requested TaskGroup Tasks.  "
                f"(Requested tasks for {self.task_group_id} but received "
                f"tasks for {r['taskgroup_id']}"
            )
            raise ValueError(msg)

        # step 3: create the associated set of futures
        futures: t.List[FuncXFuture] = []
        for task in r.get("tasks", []):
            task_uuid: str = task["id"]
            fut = FuncXFuture(task_uuid)
            self._function_future_map[task_uuid] = fut
            futures.append(fut)

        if not futures:
            log.warning(f"Received no tasks for Task Group ID: {self.task_group_id}")

        # step 4: start up polling!
        self.poller_thread.atomic_controller.increment(val=len(futures))

        # step 5: the goods for the consumer
        return futures

    def shutdown(self):
        if self.batch_enabled and self._kill_event:
            self._kill_event.set()  # Reminder: stops the batch submission thread
            self.task_outgoing.put((None, None))

        self._reset_poller()

        log.debug(f"Executor:{self.label} shutting down")


def noop():
    return


class ExecutorPollerThread:
    """This encapsulates the creation of the thread on which event loop lives,
    the instantiation of the WebSocketPollingTask onto the event loop and the
    synchronization primitives used (AtomicController)
    """

    def __init__(
        self,
        funcx_client: FuncXClient,
        function_future_map: t.Dict[str, FuncXFuture],
    ):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        function_future_map
            A mapping of task_uuid to associated FuncXFutures; used for updating
            when the upstream websocket service sends updates
        """

        self.funcx_client: FuncXClient = funcx_client
        self._function_future_map: t.Dict[str, FuncXFuture] = function_future_map
        self.eventloop = asyncio.new_event_loop()
        self.atomic_controller = AtomicController(self._start, noop)
        self.ws_handler = WebSocketPollingTask(
            self.funcx_client,
            self.eventloop,
            atomic_controller=self.atomic_controller,
            init_task_group_id=self.task_group_id,
            results_ws_uri=self.results_ws_uri,
            auto_start=False,
        )
        self._thread: t.Optional[threading.Thread] = None

    @property
    def results_ws_uri(self) -> str:
        return self.funcx_client.results_ws_uri

    @property
    def task_group_id(self) -> str:
        return self.funcx_client.session_task_group_id

    @property
    def is_running(self) -> bool:
        return self.eventloop.is_running()

    def _start(self):
        """Start the result polling thread"""
        # Currently we need to put the batch id's we launch into this queue
        # to tell the web_socket_poller to listen on them. Later we'll associate

        self.ws_handler.closed_by_main_thread = False
        self._thread = threading.Thread(
            target=self.event_loop_thread, daemon=True, name="FuncX-Poller-Thread"
        )
        self._thread.start()
        log.debug("Started web_socket_poller thread")

    def event_loop_thread(self):
        asyncio.set_event_loop(self.eventloop)
        self.eventloop.run_until_complete(self.web_socket_poller())

    async def web_socket_poller(self):
        """Start ws and listen for tasks.
        If a remote disconnect breaks the ws, close the ws and reconnect"""
        time_to_disconnect = False
        while not time_to_disconnect:
            await self.ws_handler.init_ws(start_message_handlers=False)
            time_to_disconnect = await self.ws_handler.handle_incoming(
                self._function_future_map, auto_close=True
            )
            if not time_to_disconnect:
                # handle_incoming broke from a remote side disconnect
                # we should close and re-connect
                log.debug("Attempting ws close")
                await self.ws_handler.close()
                log.debug("Attempting ws re-connect")

    def shutdown(self):
        if self.is_running:
            self.ws_handler.closed_by_main_thread = True
            asyncio.run_coroutine_threadsafe(
                self.ws_handler.close(), self.eventloop
            ).result()
            self.eventloop.stop()
