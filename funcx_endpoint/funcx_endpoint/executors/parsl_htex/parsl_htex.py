import concurrent.futures
import logging
import multiprocessing
import os
import uuid
from concurrent.futures import Future

from funcx_common import messagepack
from funcx_common.messagepack.message_types import Task
from parsl.executors.high_throughput.executor import HighThroughputExecutor

from funcx_endpoint.executors.execution_helper.helper import execute_task

logger = logging.getLogger(__name__)


class ParslHTEX(HighThroughputExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self, run_dir: str, results_passthrough: multiprocessing.Queue = None):
        self.run_dir = os.path.join(os.getcwd(), run_dir)
        self.run_id = "001"
        self.provider.script_dir = os.path.join(self.run_dir, "submit_scripts")
        os.makedirs(self.provider.script_dir, exist_ok=True)
        self.results_passthrough = results_passthrough
        super().start()

    def _future_done_callback(self, future: concurrent.futures.Future):
        """Callback posts result to results_passthrough

        Parameters
        ----------
        future: Future for which the callback is triggerd

        Returns
        -------
        None

        """
        logger.warning(f"[YADU] task:{future.task_id} completed")
        self.results_passthrough.put(future.result())

    def submit_raw(self, packed_task: bytes) -> Future:
        unpacked_task: Task = messagepack.unpack(packed_task)
        future = self.submit(execute_task, {}, unpacked_task.task_id, packed_task)

        future.task_id: uuid.UUID = unpacked_task.task_id
        future.add_done_callback(self._future_done_callback)
        return future
