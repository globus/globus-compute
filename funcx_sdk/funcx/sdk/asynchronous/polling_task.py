import asyncio
from asyncio import AbstractEventLoop
import time
from funcx.sdk.asynchronous.funcx_task import FuncXTask


class PollingTask:
    """
    Launches an asycnhio task for polling unresolved funcX tasks. Uses a work queue to
    record unresolved funcX Tasks. Pulls one off and attempts to retrieve the result.
    If this attempt fails, put the value back on the queue for later attempt.
    Use a throttle to insure we DDOS the web service
    """
    async def poll_funcx(self, queue: asyncio.Queue):
        while True:
            t = await queue.get()
            try:
                result = self.fxc.get_result(t.task_id)
                t.future.set_result(result)
            except Exception as eek:
                print(eek)
                await self.throttle()
                await queue.put(t)

    def __init__(self, fxc, loop: AbstractEventLoop, period: float,
                 max_calls_per_period: int, cooldown: float):
        """

        :param fxc: FuncXClient
            Client instance for requesting results
        :param loop: AbstractEventLoop
            Asynchio event loop to manage asynchronous calls
        :param period: float
            Number of seconds over which the throttle is observing number of calls
        :param max_calls_per_period: int
            Maximum number of calls to be allowed during a period
        :param cooldown: float
            Number of seconds to sleep if we hit the max number of calls during the
            period
        """
        self.fxc = fxc
        self.loop = loop
        self.running_tasks = asyncio.Queue()
        self.poll_task = self.loop.create_task(self.poll_funcx(self.running_tasks))

        if hasattr(time, 'monotonic'):
            self._now = time.monotonic
        else:
            self._now = time.time

        self.last_reset = self._now()
        self.num_calls = 0
        self.max_calls_per_period = max_calls_per_period
        self.cooldown = cooldown
        self.period = period

    async def throttle(self):
        """
        Apply a throttle to calls made to the funcX web service. Don't allow more than
        max_calls_period during a specific period. If this threshold is breached we
        sleep for cooldown seconds
        :return:
        """
        elapsed = self._now() - self.last_reset
        period_remaining = self.period - elapsed

        if period_remaining <= 0:
            self.num_calls = 0
            self.last_reset = self._now()

        self.num_calls += 1

        if self.num_calls > self.max_calls_per_period:
            print("Cool rown")
            await asyncio.sleep(self.cooldown)

    def put(self, task: FuncXTask):
        """
        Add a funcX task to the poller
        :param task: FuncXTask
            Task to be added
        """
        self.running_tasks.put_nowait(task)
