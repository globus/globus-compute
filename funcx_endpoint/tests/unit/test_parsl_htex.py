import concurrent.futures
import logging
import multiprocessing
import os
import random
import shutil
import time
import uuid

import pytest
from funcx_common import messagepack
from funcx_common.messagepack.message_types import TaskTransition
from funcx_common.tasks import ActorName, TaskState
from pytest import fixture

from funcx.serialize import FuncXSerializer
from funcx_endpoint.executors.parsl_htex.parsl_htex import ParslHTEX
from funcx_endpoint.executors.process_pool import ProcessPoolExecutor

from .wrapper import ez_pack_function

logger = logging.getLogger(__name__)


@fixture
def proc_pool_executor():
    executor = ProcessPoolExecutor(max_workers=2)
    queue = multiprocessing.Queue()
    executor.start(run_dir="/tmp", results_passthrough=queue)
    yield executor
    executor.shutdown()


@fixture
def parsl_htex_executor():
    executor = ParslHTEX(address="127.0.0.1")
    queue = multiprocessing.Queue()
    tempdir = "/tmp/HTEX_logs"
    os.makedirs(tempdir, exist_ok=True)
    executor.start(run_dir=tempdir, results_passthrough=queue)
    yield executor
    executor.shutdown()
    shutil.rmtree(tempdir)


def double(x):
    return x * 2


def test_result_message_packing():
    exec_start = TaskTransition(
        timestamp=time.time_ns(), state=TaskState.EXEC_START, actor=ActorName.WORKER
    )

    serializer = FuncXSerializer()
    task_id = uuid.uuid1()
    result = random.randint(0, 1000)

    exec_end = TaskTransition(
        timestamp=time.time_ns(), state=TaskState.EXEC_END, actor=ActorName.WORKER
    )
    result_message = dict(
        task_id=task_id,
        data=serializer.serialize(result),
        task_statuses=[exec_start, exec_end],
    )

    mResult = messagepack.message_types.Result(**result_message)
    assert isinstance(mResult, messagepack.message_types.Result)
    packed_result = messagepack.pack(mResult)
    assert isinstance(packed_result, bytes)

    unpacked = messagepack.unpack(packed_result)
    assert isinstance(unpacked, messagepack.message_types.Result)
    # assert unpacked.
    logger.warning(f"Type of unpacked : {unpacked}")
    assert unpacked.task_id == task_id
    assert serializer.deserialize(unpacked.data) == result


@pytest.mark.parametrize("x", ["parsl_htex", "proc_pool"])
def test_executor_basic_submit(x, parsl_htex_executor, proc_pool_executor):
    "Test executor.submit with multiple executors"
    if x == "parsl_htex":
        executor = parsl_htex_executor
    else:
        executor = proc_pool_executor

    param = random.randint(1, 100)
    future = executor.submit(double, {}, param)
    assert isinstance(future, concurrent.futures.Future)
    assert future.result() == param * 2


def test_proc_pool_basic_submit_raw(parsl_htex_executor):
    q = parsl_htex_executor.results_passthrough
    task_id = uuid.uuid1()
    serializer = FuncXSerializer()
    task_body = ez_pack_function(serializer, double, (3,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )
    future = parsl_htex_executor.submit_raw(task_message)
    packed_result = future.result()

    assert isinstance(packed_result, bytes)
    assert q.get(timeout=0.1) == packed_result
    result = messagepack.unpack(packed_result)
    assert result.task_id == task_id
    final_result = serializer.deserialize(result.data)
    assert final_result == 6, f"Expected 6, but got: {final_result}"


def test_parsl_htex_basic(parsl_htex_executor):
    q = parsl_htex_executor.results_passthrough
    task_id = uuid.uuid1()
    serializer = FuncXSerializer()
    task_body = ez_pack_function(serializer, double, (3,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )
    future = parsl_htex_executor.submit_raw(task_message)
    packed_result = future.result()

    assert isinstance(packed_result, bytes)
    assert q.get(timeout=0.1) == packed_result
    logger.warning(f"Yadu: Got {packed_result=}")
    result = messagepack.unpack(packed_result)
    logger.warning(f"Yadu: Got {result=}")
    assert result.task_id == task_id
    final_result = serializer.deserialize(result.data)
    assert final_result == 6, f"Expected 6, but got: {final_result}"
