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
from parsl.executors.high_throughput.interchange import ManagerLost
from tests.utils import double, ez_pack_function, slow_double

from funcx.serialize import FuncXSerializer
from funcx_endpoint.executors import GCExecutor, ProcessPoolExecutor

logger = logging.getLogger(__name__)


@pytest.fixture
def proc_pool_executor():
    ep_id = uuid.uuid4()
    executor = ProcessPoolExecutor(
        label="ProcessPoolExecutor", heartbeat_period=1, max_workers=2
    )
    queue = multiprocessing.Queue()
    executor.start(endpoint_id=ep_id, run_dir="/tmp", results_passthrough=queue)

    yield executor
    executor.shutdown()


@pytest.fixture
def gc_executor():
    ep_id = uuid.uuid4()
    executor = GCExecutor(
        address="127.0.0.1", heartbeat_period=1, heartbeat_threshold=1
    )
    queue = multiprocessing.Queue()
    tempdir = "/tmp/HTEX_logs"
    os.makedirs(tempdir, exist_ok=True)
    executor.start(endpoint_id=ep_id, run_dir=tempdir, results_passthrough=queue)

    yield executor
    executor.shutdown()
    shutil.rmtree(tempdir, ignore_errors=True)


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


@pytest.mark.parametrize("x", ["gc_executor", "proc_pool_executor"])
def test_executor_submit(x, gc_executor, proc_pool_executor):
    "Test executor.submit with multiple executors"
    if x == "gc_executor":
        executor = gc_executor
    else:
        executor = proc_pool_executor

    param = random.randint(1, 100)
    future = executor.submit(double, param)
    assert isinstance(future, concurrent.futures.Future)
    logger.warning(f"Got result: {future.result()}")
    assert future.result() == param * 2


@pytest.mark.parametrize("x", ["gc_executor", "proc_pool_executor"])
def test_executor_submit_raw(x, gc_executor, proc_pool_executor):
    if x == "gc_executor":
        executor = gc_executor
    else:
        executor = proc_pool_executor

    q = executor.results_passthrough
    task_id = uuid.uuid1()
    serializer = FuncXSerializer()
    task_body = ez_pack_function(serializer, double, (3,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )
    future = executor.submit_raw(task_message)
    packed_result = future.result()

    # Confirm that the future got the right answer
    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_id

    # Confirm that the same result got back though the queue
    for _i in range(3):
        packed_result_q = q.get(timeout=0.1)
        assert isinstance(
            packed_result_q, bytes
        ), "Expected bytes from the passthrough_q"

        result = messagepack.unpack(packed_result_q)
        logger.warning(f"Got message: of type {type(result)} {result=} ")
        # Handle a sneaky EPStatusReport that popped in ahead of the result
        if isinstance(result, messagepack.message_types.EPStatusReport):
            continue

        # At this point the message should be the result
        assert (
            packed_result == packed_result_q
        ), "Result from passthrough_q and future should match"

        assert result.task_id == task_id
        final_result = serializer.deserialize(result.data)
        assert final_result == 6, f"Expected 6, but got: {final_result}"
        break


def test_gc_executor_system_failure(gc_executor):
    """Test behavior of executor failure killing task"""
    param = random.randint(1, 100)
    q = gc_executor.results_passthrough
    task_id = uuid.uuid1()
    serializer = FuncXSerializer()
    # We want the task to be running when we kill the manager
    task_body = ez_pack_function(
        serializer,
        slow_double,
        (
            param,
            5,
        ),
        {},
    )
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )
    future = gc_executor.submit_raw(task_message)

    assert isinstance(future, concurrent.futures.Future)
    # Trigger a failure from managers scaling in.
    gc_executor.executor.scale_in(blocks=1)
    # We need to scale out to make sure following tests will not be affected
    gc_executor.executor.scale_out(blocks=1)
    with pytest.raises(ManagerLost):
        future.result()

    for _i in range(10):
        packed_result = q.get(timeout=1)
        assert packed_result

        result = messagepack.unpack(packed_result)
        if isinstance(result, messagepack.message_types.EPStatusReport):
            continue
        else:
            assert result.task_id == task_id
            assert result.error_details
            assert "ManagerLost" in result.data
            break
