from __future__ import annotations

import multiprocessing
import unittest.mock
import uuid

from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import Result, Task
from globus_compute_sdk import Client


class MockExecutor(unittest.mock.Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.results_passthrough: multiprocessing.Queue | None = None
        self.passthrough = True

    def start(
        self,
        endpoint_id: uuid.UUID | None = None,
        run_dir: str | None = None,
        results_passthrough: multiprocessing.Queue = None,
        funcx_client: Client = None,
    ):
        self.results_passthrough = results_passthrough
        self.funcx_client = funcx_client
        self.endpoint_id = endpoint_id
        self.run_dir = run_dir

    def submit(self, task_id: uuid.UUID, packed_task: bytes):
        task: Task = messagepack.unpack(packed_task)
        res = Result(task_id=task_id, data=task.task_buffer)
        packed_result = messagepack.pack(res)
        msg = {"task_id": str(task_id), "message": packed_result}
        self.results_passthrough.put(msg)
