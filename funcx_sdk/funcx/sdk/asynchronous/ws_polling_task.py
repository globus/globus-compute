import asyncio
import json
import logging
from asyncio import AbstractEventLoop, QueueEmpty
import dill
import websockets
from websockets.exceptions import InvalidHandshake

from funcx.sdk.asynchronous.funcx_task import FuncXTask

logger = logging.getLogger(__name__)


class WebSocketPollingTask:
    """
    """

    def __init__(self, fxc, loop: AbstractEventLoop, fx_serializer):
        """

        :param fxc: FuncXClient
            Client instance for requesting results
        :param loop: AbstractEventLoop
            Asynchio event loop to manage asynchronous calls
        """
        self.fxc = fxc
        self.loop = loop
        self.fx_serializer = fx_serializer
        self.running_batch_ids = asyncio.Queue()
        self.pending_tasks = {}

        self.loop.create_task(self.init_ws())

    async def init_ws(self):
        uri = 'ws://localhost:6000'
        headers = [self.get_auth_header()]
        try:
            self.ws = await websockets.client.connect(uri, extra_headers=headers)
        # initial Globus authentication happens during the HTTP portion of the handshake,
        # so an invalid handshake means that the user was not authenticated
        except InvalidHandshake:
            raise Exception('Failed to authenticate user. Please ensure that you are logged in.')
        self.loop.create_task(self.send_outgoing(self.running_batch_ids))
        self.loop.create_task(self.handle_incoming())

    async def send_outgoing(self, queue: asyncio.Queue):
        while True:
            batch_id = await queue.get()
            await self.ws.send(batch_id)

    async def handle_incoming(self):
        while True:
            raw_data = await self.ws.recv()
            data = json.loads(raw_data)
            task_id = data['task_id']
            if task_id in self.pending_tasks:
                task = self.pending_tasks[task_id]
                del self.pending_tasks[task_id]
                if data['result']:
                    task.set_result(self.fx_serializer.deserialize(data['result']))
                elif data['exception']:
                    r_exception = self.fx_serializer.deserialize(data['exception'])
                    task.set_exception(dill.loads(r_exception.e_value))
                else:
                    task.set_exception(Exception(data['reason']))

    def put_batch_id(self, batch_id):
        self.running_batch_ids.put_nowait(batch_id)

    def add_task(self, task: FuncXTask):
        """
        Add a funcX task
        :param task: FuncXTask
            Task to be added
        """
        self.pending_tasks[task.task_id] = task

    def get_auth_header(self):
        """
        Gets an Authorization header to be sent during the WebSocket handshake. Based on
        header setting in the Globus SDK: https://github.com/globus/globus-sdk-python/blob/main/globus_sdk/base.py

        Returns
        -------
        Key-value tuple of the Authorization header
        (key, value)
        """
        headers = dict()
        self.fxc.authorizer.set_authorization_header(headers)
        header_name = 'Authorization'
        return (header_name, headers[header_name])
