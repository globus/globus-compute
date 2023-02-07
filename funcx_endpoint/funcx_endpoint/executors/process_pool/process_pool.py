from __future__ import annotations

import logging
import multiprocessing
import uuid
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor as NativeProcessPoolExecutor

from funcx_common import messagepack
from funcx_common.messagepack.message_types import Task
from parsl.executors.status_handling import NoStatusHandlingExecutor

from funcx_endpoint.executors.execution_helper.helper import execute_task

logger = logging.getLogger(__name__)


class ProcessPoolExecutor(NoStatusHandlingExecutor):
    def __init__(self, *args, **kwargs):
        self.executor = NativeProcessPoolExecutor(*args, **kwargs)
        self.results_passthrough: multiprocessing.Queue | None = None

    def start(self, results_passthrough: multiprocessing.Queue, run_dir=None) -> None:
        """

        Parameters
        ----------
        results_passthrough: Queue to which packed results will be posted
        run_dir Not used

        Returns
        -------

        """
        self.results_passthrough = results_passthrough
        return

    def _future_done_callback(self, future: Future):
        """Callback to post result to the passthrough queue

        Parameters
        ----------
        future: Future for which the callback is triggerd

        Returns
        -------
        None

        """
        logger.warning(f"[YADU] task:{future.task_id} completed")
        self.results_passthrough.put(future.result())

    def submit(self, func, resource_specification: dict, *args, **kwargs) -> Future:
        """submits func to the executor and returns a future
        Parameters
        ----------
        func: Callable to be executed
        resource_specification: This resource_spec dict is added only to match
            Parsl's interface
        args
        kwargs

        Returns
        -------
        future
        """
        logger.warning(f"calling {func=} {args=}")
        return self.executor.submit(func, *args, **kwargs)

    def submit_raw(self, packed_task: bytes) -> Future:
        """

        Parameters
        ----------
        packed_task

        Returns
        -------

        """
        unpacked_task: Task = messagepack.unpack(packed_task)
        future = self.submit(execute_task, {}, unpacked_task.task_id, packed_task)

        future.task_id: uuid.UUID = unpacked_task.task_id
        future.add_done_callback(self._future_done_callback)
        return future

    def status_polling_interval(self) -> int:
        return 30

    def scale_out(self, blocks: int) -> list[str]:
        return []

    def scale_in(self, blocks: int) -> list[str]:
        return []

    def status(self) -> dict:
        return {}

    def shutdown(self) -> bool:
        return self.executor.shutdown()


def double(x):
    return x * 2


p = ProcessPoolExecutor(max_workers=4)
future = p.submit(double, {}, 5)
