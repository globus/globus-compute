import asyncio
import json
import logging
from asyncio import AbstractEventLoop, QueueEmpty
import dill
import websockets

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
        self.running_tasks = asyncio.Queue()
        self.pending_tasks = {}

        self.loop.create_task(self.init_ws())

    async def init_ws(self):
        uri = 'ws://localhost:6000'
        self.ws = await websockets.client.connect(uri)
        self.loop.create_task(self.send_outgoing(self.running_tasks))
        self.loop.create_task(self.handle_incoming())

    async def send_outgoing(self, queue: asyncio.Queue):
        while True:
            task = await queue.get()
            data = [task.task_id]
            await self.ws.send(json.dumps(data))

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

    def put(self, task: FuncXTask):
        """
        Add a funcX task to the poller
        :param task: FuncXTask
            Task to be added
        """
        self.running_tasks.put_nowait(task)
        self.pending_tasks[task.task_id] = task
