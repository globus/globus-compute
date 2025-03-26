import concurrent.futures
import random
import typing as t
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import Result
from globus_compute_endpoint.engines import (
    GlobusComputeEngine,
    ProcessPoolEngine,
    ThreadPoolEngine,
)
from globus_compute_endpoint.engines.base import GlobusComputeEngineBase
from tests.utils import double, get_cwd


@pytest.mark.parametrize(
    "engine_type", (ProcessPoolEngine, ThreadPoolEngine, GlobusComputeEngine)
)
def test_engine_start(
    engine_type: t.Type[GlobusComputeEngineBase], engine_runner, endpoint_uuid, tmp_path
):
    """Engine.submit should fail before engine is started"""

    engine = engine_type()
    assert not engine._engine_ready, "Engine should not be ready before start"

    engine.executor = mock.Mock(status_polling_interval=0)

    # task submit should raise Exception if it was not started
    with pytest.raises(RuntimeError):
        engine.submit(str(endpoint_uuid), b"", {})

    engine.start(endpoint_id=endpoint_uuid, run_dir=tmp_path)
    assert engine._engine_ready, "Engine should be ready after start"

    engine.shutdown()


@pytest.mark.parametrize(
    "engine_type", (ProcessPoolEngine, ThreadPoolEngine, GlobusComputeEngine)
)
def test_engine_submit(engine_type: GlobusComputeEngineBase, engine_runner):
    """Test engine.submit with multiple engines"""
    engine = engine_runner(engine_type)

    param = random.randint(1, 100)
    resource_spec: dict = {}
    future = engine._submit(double, resource_spec, param)
    assert isinstance(future, concurrent.futures.Future)

    # 5-seconds is nominally "overkill," but gc on CI appears to need (at least) >1s
    assert future.result(timeout=5) == param * 2


@pytest.mark.parametrize(
    "engine_type", (ProcessPoolEngine, ThreadPoolEngine, GlobusComputeEngine)
)
def test_engine_working_dir(
    engine_type: GlobusComputeEngineBase,
    engine_runner,
    ez_pack_task,
    serde,
    task_uuid,
):
    """working dir remains constant across multiple fn invocations
    This test requires submitting the task payload so that the execute_task
    wrapper is used which switches into the working_dir, which created
    working_dir nesting when relative paths were used.
    """
    engine = engine_runner(engine_type)

    task_args: tuple = (str(task_uuid), ez_pack_task(get_cwd), {})

    future1 = engine.submit(*task_args)
    unpacked1 = messagepack.unpack(future1.result())  # blocks; avoid race condition

    future2 = engine.submit(*task_args)  # exact same task
    unpacked2 = messagepack.unpack(future2.result())

    # data is enough for test, but in error case, be kind to dev
    assert isinstance(unpacked1, Result)
    assert isinstance(unpacked2, Result)
    cwd1 = serde.deserialize(unpacked1.data)
    cwd2 = serde.deserialize(unpacked2.data)
    assert cwd1 == cwd2, "working dir should be idempotent"


@pytest.mark.parametrize(
    "engine_type", (ProcessPoolEngine, ThreadPoolEngine, GlobusComputeEngine)
)
def test_engine_submit_internal(
    engine_type: GlobusComputeEngineBase, engine_runner, serde, task_uuid, ez_pack_task
):
    engine = engine_runner(engine_type)

    task_bytes = ez_pack_task(double, 3)
    f = engine.submit(str(task_uuid), task_bytes, resource_specification={})
    packed_result = f.result()

    # Confirm that the future got the right answer
    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, Result)
    assert result.task_id == task_uuid
    assert serde.deserialize(result.data) == 6
