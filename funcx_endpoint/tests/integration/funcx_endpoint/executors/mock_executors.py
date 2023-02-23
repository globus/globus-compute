from __future__ import annotations

import multiprocessing
import unittest.mock
import uuid

import dill
from funcx_common.messagepack.message_types import Result, Task

from funcx import FuncXClient
from funcx_endpoint.executors.high_throughput.messages import Message


class MockExecutor(unittest.mock.Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.results_passthrough: multiprocessing.Queue | None = None
        self.passthrough = True

    def start(
        self,
        endpoint_id: uuid.UUID = None,
        run_dir=None,
        results_passthrough: multiprocessing.Queue = None,
        funcx_client: FuncXClient = None,
    ):
        self.results_passthrough = results_passthrough
        self.funcx_client = funcx_client
        self.endpoint_id = endpoint_id
        self.run_dir = run_dir

    def submit_raw(self, packed_task: bytes):
        task: Task = Message.unpack(packed_task)
        res = Result(task_id=task.task_id, data=task.task_buffer)
        res = {"task_id": "abc", "message": dill.dumps(res)}
        self.results_passthrough.put(res)
