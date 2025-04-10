from __future__ import annotations

import typing as t
import unittest.mock
import uuid

from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import Result, Task
from globus_compute_endpoint.engines import GCFuture
from globus_compute_sdk import Client
from globus_compute_sdk.serialize.facade import validate_strategylike
from parsl.executors.errors import InvalidResourceSpecification


class MockEngine(unittest.mock.Mock):
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
        task_f: GCFuture,
        packed_task: bytes,
        resource_specification: t.Dict,
        result_serializers: t.Optional[t.List[str]] = None,
    ):
        task: Task = messagepack.unpack(packed_task)
        task_f.executor_task_id = 1
        task_f.job_id = 12
        task_f.block_id = 123
        res = Result(task_id=task_f.gc_task_id, data=task.task_buffer)

        # This is a hack to trigger an InvalidResourceSpecification
        if "BAD_KEY" in resource_specification:
            raise InvalidResourceSpecification({"BAD_KEY"})

        if result_serializers:
            for serializer in result_serializers:
                validate_strategylike(serializer)

        task_f.set_result(messagepack.pack(res))
