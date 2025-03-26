from __future__ import annotations

import typing as t
import unittest.mock
import uuid
from concurrent.futures import Future

from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import Result, Task
from globus_compute_sdk import Client
from parsl.executors.errors import InvalidResourceSpecification


class MockExecutor(unittest.mock.Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.passthrough = True
        self.executor_exception = False

    def start(
        self,
        endpoint_id: uuid.UUID | None = None,
        run_dir: str | None = None,
        funcx_client: Client = None,
    ):
        self.funcx_client = funcx_client
        self.endpoint_id = endpoint_id
        self.run_dir = run_dir

    def submit(
        self,
        task_id: uuid.UUID,
        packed_task: bytes,
        resource_specification: t.Dict,
    ):
        task: Task = messagepack.unpack(packed_task)
        res = Result(task_id=task_id, data=task.task_buffer)

        # This is a hack to trigger an InvalidResourceSpecification
        if "BAD_KEY" in resource_specification:
            raise InvalidResourceSpecification({"BAD_KEY"})

        f = Future()
        f.gc_task_id = task_id
        f.set_result(messagepack.pack(res))
        return f
