import concurrent.futures
import logging
import random
import time
import uuid

import pytest
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import TaskTransition
from globus_compute_common.tasks import ActorName, TaskState
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.endpoint.config.utils import serialize_config
from globus_compute_endpoint.engines import (
    GlobusComputeEngine,
    HighThroughputEngine,
    ProcessPoolEngine,
    ThreadPoolEngine,
)
from globus_compute_endpoint.engines.base import GlobusComputeEngineBase
from globus_compute_sdk.serialize import ComputeSerializer
from parsl.executors.high_throughput.interchange import ManagerLost
from tests.utils import double, ez_pack_function, slow_double

logger = logging.getLogger(__name__)


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


@pytest.mark.parametrize(
    "engine_type", (ProcessPoolEngine, ThreadPoolEngine, GlobusComputeEngine)
)
def test_engine_submit(engine_type: GlobusComputeEngineBase, engine_runner):
    """Test engine.submit with multiple engines"""
    engine = engine_runner(engine_type)

    param = random.randint(1, 100)
    future = engine._submit(double, param)
    assert isinstance(future, concurrent.futures.Future)

    # 5-seconds is nominally "overkill," but gc on CI appears to need (at least) >1s
    assert future.result(timeout=5) == param * 2


@pytest.mark.parametrize(
    "engine_type", (ProcessPoolEngine, ThreadPoolEngine, GlobusComputeEngine)
)
def test_engine_submit_internal(engine_type: GlobusComputeEngineBase, engine_runner):
    engine = engine_runner(engine_type)

    q = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, double, (3,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )
    future = engine.submit(str(task_id), task_message)
    packed_result = future.result()

    # Confirm that the future got the right answer
    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_id

    # Confirm that the same result got back though the queue
    for _i in range(3):
        q_msg = q.get(timeout=0.1)
        assert isinstance(q_msg, dict)

        packed_result_q = q_msg["message"]
        result = messagepack.unpack(packed_result_q)
        # Handle a sneaky EPStatusReport that popped in ahead of the result
        if isinstance(result, messagepack.message_types.EPStatusReport):
            continue

        passed_task_id = q_msg["task_id"]
        # At this point the message should be the result
        assert (
            packed_result == packed_result_q
        ), "Result from passthrough_q and future should match"

        assert passed_task_id == str(task_id)
        assert result.task_id == task_id
        final_result = serializer.deserialize(result.data)
        assert final_result == 6, f"Expected 6, but got: {final_result}"
        break


def test_proc_pool_engine_not_started():
    engine = ProcessPoolEngine(heartbeat_period_s=1, max_workers=2)

    with pytest.raises(AssertionError) as pyt_exc:
        engine.submit(double, 10)
    assert "engine has not been started" in str(pyt_exc)

    with pytest.raises(AssertionError):
        engine.get_status_report()
    assert "engine has not been started" in str(pyt_exc)


def test_gc_engine_system_failure(engine_runner):
    """Test behavior of engine failure killing task"""
    engine = engine_runner(GlobusComputeEngine)
    param = random.randint(1, 100)
    q = engine.results_passthrough
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
    future = engine.submit(str(task_id), task_message)

    assert isinstance(future, concurrent.futures.Future)
    # Trigger a failure from managers scaling in.
    engine.executor.scale_in(blocks=1)
    # We need to scale out to make sure following tests will not be affected
    engine.executor.scale_out(blocks=1)
    with pytest.raises(ManagerLost):
        future.result()

    for _i in range(10):
        q_msg = q.get(timeout=1)
        assert q_msg

        packed_result = q_msg["message"]
        result = messagepack.unpack(packed_result)
        if isinstance(result, messagepack.message_types.EPStatusReport):
            continue

        assert result.task_id == task_id
        assert result.error_details
        assert "ManagerLost" in result.data
        break


@pytest.mark.parametrize("engine_type", (GlobusComputeEngine, HighThroughputEngine))
def test_serialized_engine_config_has_provider(engine_type: GlobusComputeEngineBase):
    ep_config = Config(executors=[engine_type()])

    res = serialize_config(ep_config)
    executor = res["executors"][0].get("executor") or res["executors"][0]

    assert executor.get("provider")
