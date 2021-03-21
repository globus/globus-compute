import asyncio
import json
import logging
from asyncio import AbstractEventLoop, QueueEmpty
import websockets

from funcx.sdk.asynchronous.funcx_task import FuncXTask

logger = logging.getLogger(__name__)


class WebSocketPollingTask:
    """
    """

    async def init_ws(self):
        uri = 'ws://localhost:6000'
        self.ws = await websockets.client.connect(uri)

    async def send_outgoing(self, queue: asyncio.Queue):
        while True:
            task = await queue.get()
            data = [task.task_id]
            await self.ws.send(json.dumps(data))

    async def handle_incoming(self):
        while True:
            raw_data = await self.ws.recv()
            data = json.loads(raw_data)
            print(data)

    def __init__(self, fxc, loop: AbstractEventLoop):
        """

        :param fxc: FuncXClient
            Client instance for requesting results
        :param loop: AbstractEventLoop
            Asynchio event loop to manage asynchronous calls
        """
        self.fxc = fxc
        self.loop = loop
        self.running_tasks = asyncio.Queue()

        self.loop.run_until_complete(self.init_ws())
        self.loop.create_task(self.send_outgoing(self.running_tasks))
        self.loop.create_task(self.handle_incoming())

    def put(self, task: FuncXTask):
        """
        Add a funcX task to the poller
        :param task: FuncXTask
            Task to be added
        """
        self.running_tasks.put_nowait(task)
