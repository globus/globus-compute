import os
import uuid
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import (
    GlobusComputeEngine,
    ProcessPoolEngine,
    ThreadPoolEngine,
)
from globus_compute_endpoint.engines.helper import execute_task
from parsl.executors import HighThroughputExecutor
from tests.utils import get_cwd


@pytest.fixture()
def reset_cwd():
    cwd = os.getcwd()
    yield
    os.chdir(cwd)


@pytest.mark.parametrize(
    "engine",
    (GlobusComputeEngine(address="127.0.0.1"), ThreadPoolEngine(), ProcessPoolEngine()),
)
def test_set_working_dir_default(engine, tmp_path):
    """Verify that working dir is set to tasks dir in the run_dir by default for all
    engines
    """
    engine.set_working_dir(tmp_path)
    assert engine.working_dir == os.path.join(tmp_path, "tasks_working_dir")
    assert os.path.isabs(engine.working_dir) is True


@pytest.mark.parametrize(
    "engine",
    (GlobusComputeEngine(address="127.0.0.1"), ThreadPoolEngine(), ProcessPoolEngine()),
)
def test_set_working_dir_called(engine, tmp_path, endpoint_uuid):
    """Verify that set_working_dir is called when engine.start() is called"""
    engine.executor = mock.Mock()
    engine.executor.status_polling_interval = 0
    engine.set_working_dir = mock.Mock(spec=engine.set_working_dir)

    engine.start(endpoint_id=endpoint_uuid, run_dir=tmp_path)
    assert engine.set_working_dir.called


@pytest.mark.parametrize(
    "engine",
    (GlobusComputeEngine(address="127.0.0.1"), ThreadPoolEngine(), ProcessPoolEngine()),
)
def test_set_working_dir_relative(engine, tmp_path):
    """Working_dir should be absolute and set relative to the endpoint run_dir"""
    os.chdir(tmp_path)
    engine.working_dir = "tasks_working_dir"
    engine.set_working_dir(run_dir="foo")
    assert engine.working_dir == os.path.join(tmp_path, "foo", "tasks_working_dir")
    assert os.path.isabs(engine.working_dir) is True


def test_default_working_dir(tmp_path):
    """Test working_dir relative to run_dir"""
    gce = GlobusComputeEngine(
        address="127.0.0.1",
    )
    gce.executor.start = mock.MagicMock(spec=HighThroughputExecutor.start)
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)
    assert gce.run_dir == gce.executor.run_dir
    assert gce.working_dir == os.path.join(tmp_path, "tasks_working_dir")


def test_relative_working_dir(tmp_path):
    """Test working_dir relative to run_dir"""
    gce = GlobusComputeEngine(
        address="127.0.0.1",
        working_dir="relative_path",
    )
    gce.executor.start = mock.MagicMock(spec=HighThroughputExecutor.start)
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)
    assert gce.run_dir == gce.executor.run_dir
    assert gce.working_dir == os.path.join(tmp_path, "relative_path")


def test_absolute_working_dir(tmp_path):
    """Test absolute path for working_dir"""
    gce = GlobusComputeEngine(
        address="127.0.0.1",
        working_dir="/absolute/path",
    )
    gce.executor.start = mock.MagicMock(spec=HighThroughputExecutor.start)
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)
    assert gce.run_dir == gce.executor.run_dir
    assert gce.working_dir == "/absolute/path"


def test_submit_pass(tmp_path, task_uuid):
    """Test absolute path for working_dir"""
    gce = GlobusComputeEngine(
        address="127.0.0.1",
    )
    gce.executor.start = mock.Mock(spec=HighThroughputExecutor.start)
    gce.executor.submit = mock.Mock(spec=HighThroughputExecutor.submit)
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)

    gce.submit(
        task_id=task_uuid,
        packed_task=b"PACKED_TASK",
        resource_specification={},
    )

    gce.executor.submit.assert_called()
    flag = False
    for call in gce.executor.submit.call_args_list:
        args, kwargs = call
        assert "run_dir" in kwargs
        assert kwargs["run_dir"] == os.path.join(tmp_path, "tasks_working_dir")
        flag = True
    assert flag, "Call args parsing failed, did not find run_dir"


def test_execute_task_working_dir(
    tmp_path, reset_cwd, serde, endpoint_uuid, task_uuid, ez_pack_task
):
    assert os.getcwd() != str(tmp_path)
    task_bytes = ez_pack_task(get_cwd)
    packed_result = execute_task(task_uuid, task_bytes, endpoint_uuid, run_dir=tmp_path)

    message = messagepack.unpack(packed_result)
    assert message.task_id == task_uuid
    assert message.data

    result = serde.deserialize(message.data)
    assert result == os.fspath(tmp_path)
    assert os.getcwd() == os.fspath(tmp_path)


def test_sandbox(tmp_path, reset_cwd, serde, endpoint_uuid, task_uuid, ez_pack_task):
    os.chdir(tmp_path)

    task_bytes = ez_pack_task(get_cwd)
    packed_result = execute_task(
        task_uuid,
        task_bytes,
        endpoint_uuid,
        run_dir=tmp_path,
        run_in_sandbox=True,
    )

    message = messagepack.unpack(packed_result)
    assert message.task_id == task_uuid
    assert message.data
    result = serde.deserialize(message.data)

    assert result == os.path.join(tmp_path, str(task_uuid))
