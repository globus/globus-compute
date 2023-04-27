import concurrent.futures
import logging
import multiprocessing
import os
import random
import shutil
import time
import uuid
from queue import Queue

import pytest
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import TaskTransition
from globus_compute_common.tasks import ActorName, TaskState
from globus_compute_endpoint.engines import (
    GlobusComputeEngine,
    ProcessPoolEngine,
    ThreadPoolEngine,
)
from globus_compute_sdk.serialize import ComputeSerializer
from parsl.executors.high_throughput.interchange import ManagerLost
from tests.utils import double, ez_pack_function, slow_double

logger = logging.getLogger(__name__)


@pytest.fixture
def proc_pool_engine():
    ep_id = uuid.uuid4()
    engine = ProcessPoolEngine(
        label="ProcessPoolEngine", heartbeat_period_s=1, max_workers=2
    )
    queue = multiprocessing.Queue()
    engine.start(endpoint_id=ep_id, run_dir="/tmp", results_passthrough=queue)

    yield engine
    engine.shutdown()


@pytest.fixture
def thread_pool_engine():
    ep_id = uuid.uuid4()
    engine = ThreadPoolEngine(heartbeat_period_s=1, max_workers=2)

    queue = Queue()
    engine.start(endpoint_id=ep_id, run_dir="/tmp", results_passthrough=queue)

    yield engine
    engine.shutdown()


@pytest.fixture
def gc_engine():
    ep_id = uuid.uuid4()
    engine = GlobusComputeEngine(
        address="127.0.0.1", heartbeat_period_s=1, heartbeat_threshold=1
    )
    queue = multiprocessing.Queue()
    tempdir = "/tmp/HTEX_logs"
    os.makedirs(tempdir, exist_ok=True)
    engine.start(endpoint_id=ep_id, run_dir=tempdir, results_passthrough=queue)

    yield engine
    engine.shutdown()
    shutil.rmtree(tempdir, ignore_errors=True)


def test_result_message_packing():
    exec_start = TaskTransition(
        timestamp=time.time_ns(), state=TaskState.EXEC_START, actor=ActorName.WORKER
    )

    serializer = ComputeSerializer()
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


# Skipping "gc_engine" since parsl.htex.submit has an
# unreliable serialization method.
# @pytest.mark.parametrize("x", ["gc_engine"])
@pytest.mark.parametrize("x", ["proc_pool_engine"])
def test_engine_submit(x, gc_engine, proc_pool_engine):
    "Test engine.submit with multiple engines"
    if x == "gc_engine":
        engine = gc_engine
    else:
        engine = proc_pool_engine

    param = random.randint(1, 100)
    future = engine._submit(double, param)
    assert isinstance(future, concurrent.futures.Future)
    logger.warning(f"Got result: {future.result()}")
    assert future.result() == param * 2


@pytest.mark.parametrize("x", ["gc_engine", "proc_pool_engine"])
def test_engine_submit_internal(x, gc_engine, proc_pool_engine):
    if x == "gc_engine":
        engine = gc_engine
    else:
        engine = proc_pool_engine

    q = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, double, (3,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )
    future = engine.submit(task_id, task_message)
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


def test_gc_engine_system_failure(gc_engine):
    """Test behavior of engine failure killing task"""
    param = random.randint(1, 100)
    q = gc_engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
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
    future = gc_engine.submit(task_id, task_message)

    assert isinstance(future, concurrent.futures.Future)
    # Trigger a failure from managers scaling in.
    gc_engine.executor.scale_in(blocks=1)
    # We need to scale out to make sure following tests will not be affected
    gc_engine.executor.scale_out(blocks=1)
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
