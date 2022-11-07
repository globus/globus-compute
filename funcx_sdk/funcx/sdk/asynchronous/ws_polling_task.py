from __future__ import annotations

import asyncio
import json
import logging
import typing as t
from asyncio import AbstractEventLoop

# import from `websockets.client`, see:
#   https://github.com/aaugustin/websockets/issues/940
import websockets.client
from websockets.exceptions import (
    ConnectionClosedOK,
    InvalidHandshake,
    InvalidStatusCode,
)

from funcx.errors import FuncxTaskExecutionFailed
from funcx.sdk.asynchronous.funcx_future import FuncXFuture
from funcx.sdk.asynchronous.funcx_task import FuncXTask

log = logging.getLogger(__name__)

# Add extra allowance for result wrappers
DEFAULT_RESULT_SIZE_LIMIT_MB = 10
DEFAULT_RESULT_SIZE_LIMIT_B = (DEFAULT_RESULT_SIZE_LIMIT_MB + 1) * 1024 * 1024


class WebSocketPollingTask:
    """The WebSocketPollingTask is used by the FuncXExecutor and the FuncXClient
    to asynchronously listen to a stream of results. It uses a synchronized counter
    to identify when there are no more tasks and exit to avoid blocking the main thread
    from exiting.
    """

    def __init__(
        self,
        funcx_client,
        loop: AbstractEventLoop,
        atomic_controller=None,
        init_task_group_id: str | None = None,
        results_ws_uri: str = "wss://api2.funcx.org/ws/v2/",
        auto_start: bool = True,
    ):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        loop : event loop
            The asnycio event loop that the WebSocket client will run on

        atomic_controller: AtomicController object
            A synchronized counter object used to identify when there are 0 tasks
            remaining and to exit the polling loop.
            An atomic_controller is required when this is called from a non-async
            environment.

        init_task_group_id : str
            Optional task_group_id UUID string that the WebSocket client
            can immediately poll for after initialization

        results_ws_uri : str
            Web sockets URI for the results.
            Default: wss://api2.funcx.org/ws/v2

        auto_start : Bool
            Set this to start the WebSocket client immediately.
            Otherwise init_ws must be called.
            Default: True
        """
        self.funcx_client = funcx_client
        self.loop = loop
        self.atomic_controller = atomic_controller
        self.init_task_group_id = init_task_group_id
        self.results_ws_uri = results_ws_uri
        self.auto_start = auto_start
        self.running_task_group_ids = set()
        # add the initial task group id, as this will be sent to
        # the WebSocket server immediately
        self.running_task_group_ids.add(self.init_task_group_id)

        # Set event loop explicitly since event loop can only be fetched automatically
        # in main thread when batch submission is enabled, the task submission is in a
        # new thread
        asyncio.set_event_loop(self.loop)
        self.task_group_ids_queue: asyncio.Queue[str] = asyncio.Queue()
        self.pending_tasks: t.Dict[str, FuncXTask] = {}
        self.unknown_results: t.Dict[str, t.Any] = {}

        self.closed_by_main_thread = False
        self._ws: t.Optional[websockets.client.WebSocketClientProtocol] = None

        if auto_start:
            self.loop.create_task(self.init_ws())

    @property
    def ws(self) -> websockets.client.WebSocketClientProtocol:
        if self._ws is None:
            raise ValueError("cannot use websocket client without init")
        return self._ws

    async def init_ws(self, start_message_handlers=True):
        headers = [self.get_auth_header()]
        try:
            self._ws = await websockets.client.connect(
                self.results_ws_uri,
                extra_headers=headers,
                max_size=DEFAULT_RESULT_SIZE_LIMIT_B,
            )
        # initial Globus authentication happens during the HTTP portion of the
        # handshake, so an invalid handshake means that the user was not authenticated
        except InvalidStatusCode as e:
            if e.status_code == 404:
                raise Exception(
                    "WebSocket service responsed with a 404. "
                    "Please ensure you set the correct results_ws_uri"
                )
            raise
        except InvalidHandshake:
            raise Exception(
                "Failed to authenticate user. Please ensure that you are logged in."
            )

        if self.init_task_group_id:
            await self.ws.send(self.init_task_group_id)

        if start_message_handlers:
            self.loop.create_task(self.send_outgoing(self.task_group_ids_queue))
            self.loop.create_task(self.handle_incoming(self.pending_tasks))

    async def send_outgoing(self, queue: asyncio.Queue):
        while True:
            task_group_id = await queue.get()
            await self.ws.send(task_group_id)

    async def handle_incoming(self, pending_futures, auto_close=False) -> bool:
        """

        Parameters
        ----------
        pending_futures
        auto_close

        Returns
        -------
        True -- If connection is closing from internal shutdown process
        False -- External disconnect - to be handled by reconnect logic
        """
        while not self.closed_by_main_thread:
            try:
                raw_data = await asyncio.wait_for(self.ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            except ConnectionClosedOK:
                if not self.closed_by_main_thread:
                    log.info("WebSocket connection closed by remote-side")
                    return False
            else:
                data = json.loads(raw_data)
                task_id = data.get("task_id")
                if task_id:
                    task_fut = pending_futures.pop(task_id, None)
                    if task_fut is not None and await self.set_result(task_fut, data):
                        return True
                    else:
                        # This scenario occurs rarely using non-batching mode, but quite
                        # often in batching mode.
                        #
                        # When submitting tasks in batch with batch_run, some task
                        # results may be received by websocket before the response of
                        # batch_run, and pending_futures do not have the futures for the
                        # tasks yet.  We store these in unknown_results and process when
                        # their futures are ready.
                        self.unknown_results[task_id] = data
                else:
                    # This is not an expected case.  If upstream does not return a
                    # task_id, then we have a larger error in play.  Time to shut down
                    # (annoy the user!) and field the requisite bug reports.
                    errmsg = (
                        f"Upstream error: {data.get('exception', '(no reason given!)')}"
                        f"\nShutting down connection."
                    )
                    log.error(errmsg)
                    for fut in pending_futures.values():
                        if not fut.done():
                            fut.cancel()
                    return True

            # Handle the results received but not processed before
            unprocessed_task_ids = self.unknown_results.keys() & pending_futures.keys()
            for task_id in unprocessed_task_ids:
                task_fut = pending_futures.pop(task_id)
                data = self.unknown_results.pop(task_id)
                if await self.set_result(task_fut, data):
                    return True

        log.info("WebSocket connection closed by main thread")
        return True

    async def set_result(self, task_fut: FuncXFuture, task_data: t.Dict):
        """Sets the result of a future with given task_id in the pending_futures map,
        then decrement the atomic counter and close the WebSocket connection if needed

        Parameters
        ----------
        task_fut : FuncXFuture
            Task future for which to parse and set task_data


        task_data : dict
            Dict containing result/exception that should be set to future

        Returns
        -------
        bool
            True if the WebSocket connection has closed and the thread can
            exit, False otherwise
        """
        try:
            status = str(task_data.get("status")).lower()
            if status == "success" and "result" in task_data:
                task_fut.set_result(
                    self.funcx_client.fx_serializer.deserialize(task_data["result"])
                )
            elif "exception" in task_data:
                task_fut.set_exception(
                    FuncxTaskExecutionFailed(
                        task_data["exception"], task_data["completion_t"]
                    )
                )
            else:
                msg = f"Data contained neither result nor exception: {task_data}"
                task_fut.set_exception(Exception(msg))
        except Exception as exc:
            log.exception("Handling unexpected exception while setting results")
            task_exc = Exception(
                f"Malformed or unexpected data structure.  Task data: {task_data}",
            )
            task_exc.__cause__ = exc
            task_fut.set_exception(task_exc)

        # When the counter hits 0 we always exit. This guarantees that if the counter
        # increments to 1 on the executor, this handler needs to be restarted.
        if self.atomic_controller is not None:
            count = self.atomic_controller.decrement()
            # Only close when count == 0 and unknown_results are empty
            if count == 0 and len(self.unknown_results) == 0:
                await self.close()
                return True
        return False

    async def close(self):
        """Close underlying web-sockets, does not stop listeners directly"""
        if self._ws:
            await self._ws.close()
            self._ws = None

    def put_task_group_id(self, task_group_id):
        # prevent the task_group_id from being sent to the WebSocket server
        # multiple times
        if task_group_id not in self.running_task_group_ids:
            self.running_task_group_ids.add(task_group_id)
            self.task_group_ids_queue.put_nowait(task_group_id)

    def add_task(self, task: FuncXTask):
        """
        Add a funcX task
        :param task: FuncXTask
            Task to be added
        """
        self.pending_tasks[task.task_id] = task

    def get_auth_header(self) -> t.Tuple[str, t.Optional[str]]:
        """
        Gets an Authorization header to be sent during the WebSocket handshake.

        Returns
        -------
        Key-value tuple of the Authorization header
        (key, value)
        """
        # FIXME: this model, in which we inspect the client in order to get
        # authorization information, is very bad. We should instead use an authorizer or
        # other object here, if that is what's appropriate
        authz_value = self.funcx_client.web_client.authorizer.get_authorization_header()
        header_name = "Authorization"
        return header_name, authz_value
