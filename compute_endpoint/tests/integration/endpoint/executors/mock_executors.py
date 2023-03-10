from __future__ import annotations

import multiprocessing
import unittest.mock

import dill
from globus_compute_common.messagepack.message_types import Result, Task
from globus_compute_endpoint.executors.high_throughput.messages import Message
from globus_compute_sdk import Client


class MockExecutor(unittest.mock.Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.results_passthrough: multiprocessing.Queue | None = None
        self.passthrough = True

    def start(
        self,
        results_passthrough: multiprocessing.Queue = None,
        funcx_client: Client = None,
    ):
        self.results_passthrough = results_passthrough
        self.funcx_client = funcx_client

    def submit_raw(self, packed_task: bytes):
        task: Task = Message.unpack(packed_task)
        res = Result(task_id=task.task_id, data=task.task_buffer)
        res = {"task_id": "abc", "message": dill.dumps(res)}
        self.results_passthrough.put(res)
