import argparse
import asyncio
import atexit
import concurrent
import logging
import threading
import time
from concurrent.futures import Future

from funcx.sdk.asynchronous.ws_polling_task import WebSocketPollingTask

logger = logging.getLogger("asyncio")


class AtomicController:
    """This is used to synchronize between the FuncXExecutor which starts
    WebSocketPollingTasks and the WebSocketPollingTask which closes itself when there
    are 0 tasks.
    """

    def __init__(self, start_callback, stop_callback, init_value=0):
        self._value = 0
        self.lock = threading.Lock()
        self.start_callback = start_callback
        self.stop_callback = stop_callback

    def increment(self):
        with self.lock:
            if self._value == 0:
                self.start_callback()
            self._value += 1

    def decrement(self):
        with self.lock:
            self._value -= 1
            if self._value == 0:
                self.stop_callback()
            return self._value

    def value(self):
        with self.lock:
            return self._value

    def __repr__(self):
        return f"AtomicController value:{self._value}"


class FuncXExecutor(concurrent.futures.Executor):
    """Extends the concurrent.futures.Executor class to layer this interface
    over funcX. The executor returns future objects that are asynchronously
    updated with results by the WebSocketPollingTask using a websockets connection
    to the hosted funcx-websocket-service.
    """

    def __init__(self, funcx_client, label: str = "FuncXExecutor"):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        results_ws_uri : str
            Web sockets URI for the results

        label : str
            Optional string label to name the executor.
            Default: 'FuncXExecutor'
        """

        self.funcx_client = funcx_client
        self.results_ws_uri = self.funcx_client.results_ws_uri
        self.label = label
        self._tasks = {}
        self._function_registry = {}
        self._function_future_map = {}
        self.task_group_id = (
            self.funcx_client.session_task_group_id
        )  # we need to associate all batch launches with this id

        self.poller_thread = ExecutorPollerThread(
            self.funcx_client,
            self._function_future_map,
            self.results_ws_uri,
            self.task_group_id,
        )
        atexit.register(self.shutdown)

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
        Future : concurrent.futures.Future
            A future object
        """

        if function not in self._function_registry:
            # Please note that this is a partial implementation, not all function
            # registration options are fleshed out here.
            logger.debug("Function:{function} is not registered. Registering")
            function_uuid = self.funcx_client.register_function(
                function, function_name=function.__name__, container_uuid=container_uuid
            )
            self._function_registry[function] = function_uuid
            logger.debug(f"Function registered with id:{function_uuid}")
        assert endpoint_id is not None, "endpoint_id key-word argument must be set"

        batch = self.funcx_client.create_batch(task_group_id=self.task_group_id)
        batch.add(
            *args,
            endpoint_id=endpoint_id,
            function_id=self._function_registry[function],
            **kwargs,
        )
        r = self.funcx_client.batch_run(batch)
        logger.debug(f"Batch submitted to task_group: {self.task_group_id}")

        task_uuid = r[0]
        logger.debug(f"Waiting on results for task ID: {task_uuid}")
        # There's a potential for a race-condition here where the result reaches
        # the poller before the future is added to the future_map
        self._function_future_map[task_uuid] = Future()

        self.poller_thread.atomic_controller.increment()
        res = self._function_future_map[task_uuid]

        if not self.poller_thread or self.poller_thread.ws_handler.closed:
            self.poller_thread = ExecutorPollerThread(
                self.funcx_client,
                self._function_future_map,
                self.results_ws_uri,
                self.task_group_id,
            )
        return res

    def shutdown(self):
        if self.poller_thread:
            self.poller_thread.shutdown()
        logger.debug(f"Executor:{self.label} shutting down")


def noop():
    return


class ExecutorPollerThread:
    """This encapsulates the creation of the thread on which event loop lives,
    the instantiation of the WebSocketPollingTask onto the event loop and the
    synchronization primitives used (AtomicController)
    """

    def __init__(
        self, funcx_client, _function_future_map, results_ws_uri, task_group_id
    ):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        results_ws_uri : str
            Web sockets URI for the results
        """

        self.funcx_client = funcx_client
        self.results_ws_uri = results_ws_uri
        self._function_future_map = _function_future_map
        self.task_group_id = task_group_id
        self.eventloop = None
        self.atomic_controller = AtomicController(self.start, noop)
        self.ws_handler = None

    def start(self):
        """Start the result polling thread"""
        # Currently we need to put the batch id's we launch into this queue
        # to tell the web_socket_poller to listen on them. Later we'll associate

        eventloop = asyncio.new_event_loop()
        self.eventloop = eventloop
        self.ws_handler = WebSocketPollingTask(
            self.funcx_client,
            eventloop,
            atomic_controller=self.atomic_controller,
            init_task_group_id=self.task_group_id,
            results_ws_uri=self.results_ws_uri,
            auto_start=False,
        )
        self.thread = threading.Thread(target=self.event_loop_thread, args=(eventloop,))
        self.thread.start()
        logger.debug("Started web_socket_poller thread")

    def event_loop_thread(self, eventloop):
        asyncio.set_event_loop(eventloop)
        eventloop.run_until_complete(self.web_socket_poller())

    async def web_socket_poller(self):
        # TODO: if WebSocket connection fails, we should either retry connecting and back off
        # or we should set an exception to all of the outstanding futures
        await self.ws_handler.init_ws(start_message_handlers=False)
        await self.ws_handler.handle_incoming(
            self._function_future_map, auto_close=True
        )

    def shutdown(self):
        if self.ws_handler is None:
            return

        ws = self.ws_handler.ws
        if ws:
            ws_close_future = asyncio.run_coroutine_threadsafe(
                ws.close(), self.eventloop
            )
            ws_close_future.result()


def double(x):
    return x * 2


if __name__ == "__main__":
    from funcx import FuncXClient

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--service_url",
        default="http://localhost:5000/v2",
        help="URL at which the funcx-web-service is hosted",
    )
    parser.add_argument(
        "-e",
        "--endpoint_id",
        required=True,
        help="Target endpoint to send functions to",
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Count of apps to launch"
    )
    args = parser.parse_args()

    endpoint_id = args.endpoint_id

    # set_stream_logger()
    fx = FuncXExecutor(FuncXClient(funcx_service_address=args.service_url))

    print("In main")
    endpoint_id = args.endpoint_id
    future = fx.submit(double, 5, endpoint_id=endpoint_id)
    print("Got future back : ", future)

    for _i in range(5):
        time.sleep(0.2)
        # Non-blocking check whether future is done
        print("Is the future done? :", future.done())

    print("Blocking for result")
    x = future.result()  # <--- This is a blocking call
    print("Result : ", x)

    # fx.shutdown()
