import asyncio
import json
import logging
from asyncio import AbstractEventLoop

import dill
import websockets
from websockets.exceptions import (
    ConnectionClosedOK,
    InvalidHandshake,
    InvalidStatusCode,
)

from funcx.sdk.asynchronous.funcx_task import FuncXTask

log = logging.getLogger(__name__)


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
        init_task_group_id: str = None,
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
        self.task_group_ids_queue = asyncio.Queue()
        self.pending_tasks = {}
        self.unknown_results = {}

        self.closed_by_main_thread = False
        self.ws = None

        if auto_start:
            self.loop.create_task(self.init_ws())

    async def init_ws(self, start_message_handlers=True):
        headers = [self.get_auth_header()]
        try:
            self.ws = await websockets.connect(
                self.results_ws_uri, extra_headers=headers
            )
        # initial Globus authentication happens during the HTTP portion of the
        # handshake, so an invalid handshake means that the user was not authenticated
        except InvalidStatusCode as e:
            if e.status_code == 404:
                raise Exception(
                    "WebSocket service responsed with a 404. "
                    "Please ensure you set the correct results_ws_uri"
                )
            else:
                raise e
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
        while True:
            try:
                raw_data = await asyncio.wait_for(self.ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            except ConnectionClosedOK:
                if self.closed_by_main_thread:
                    log.info("WebSocket connection closed by main thread")
                    return True
                else:
                    log.info("WebSocket connection closed by remote-side")
                    return False
            else:
                data = json.loads(raw_data)
                task_id = data["task_id"]
                if task_id in pending_futures:
                    if await self.set_result(task_id, data, pending_futures):
                        return True
                else:
                    # This scenario occurs rarely using non-batching mode, but quite
                    # often in batching mode.
                    #
                    # When submitting tasks in batch with batch_run, some task results
                    # may be received by websocket before the  response of batch_run,
                    # and pending_futures do not have the futures for the tasks yet.
                    # We store these in unknown_results and process when their futures
                    # are ready.
                    self.unknown_results[task_id] = data

            # Handle the results received but not processed before
            unprocessed_task_ids = self.unknown_results.keys() & pending_futures.keys()
            for task_id in unprocessed_task_ids:
                data = self.unknown_results.pop(task_id)
                if await self.set_result(task_id, data, pending_futures):
                    return True

    async def set_result(self, task_id, data, pending_futures):
        """Sets the result of a future with given task_id in the pending_futures map,
        then decrement the atomic counter and close the WebSocket connection if needed

        Parameters
        ----------
        task_id : str
            Task ID of the future to set the result for

        data : dict
            Dict containing result/exception that should be set to future

        pending_futures : dict
            Dict of task_id keys that map to their futures

        Returns
        -------
        bool
            True if the WebSocket connection has closed and the thread can
            exit, False otherwise
        """
        future = pending_futures.pop(task_id)
        try:
            if data["result"]:
                future.set_result(
                    self.funcx_client.fx_serializer.deserialize(data["result"])
                )
            elif data["exception"]:
                r_exception = self.funcx_client.fx_serializer.deserialize(
                    data["exception"]
                )
                future.set_exception(dill.loads(r_exception.e_value))
            else:
                future.set_exception(Exception(data["reason"]))
        except Exception:
            log.exception("Caught unexpected exception while setting results")

        # When the counter hits 0 we always exit. This guarantees that that if the
        # counter increments to 1 on the executor, this handler needs to be restarted.
        if self.atomic_controller is not None:
            count = self.atomic_controller.decrement()
            # Only close when count == 0 and unknown_results are empty
            if count == 0 and len(self.unknown_results) == 0:
                await self.ws.close()
                self.ws = None
                return True
        return False

    async def close(self):
        """Close underlying web-sockets, does not stop listeners directly"""
        await self.ws.close()
        self.ws = None

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

    def get_auth_header(self):
        """
        Gets an Authorization header to be sent during the WebSocket handshake.

        Returns
        -------
        Key-value tuple of the Authorization header
        (key, value)
        """
        # TODO: under SDK v3 this will be
        #
        #   return (
        #       "Authorization",
        #       self.funcx_client.authorizer.get_authorization_header()`
        #   )
        headers = dict()
        self.funcx_client.authorizer.set_authorization_header(headers)
        header_name = "Authorization"
        return (header_name, headers[header_name])
