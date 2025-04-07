import os
import uuid
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import (
    GCFuture,
    GlobusComputeEngine,
    ProcessPoolEngine,
    ThreadPoolEngine,
)
from globus_compute_endpoint.engines.helper import execute_task
from tests.utils import get_cwd


@pytest.fixture
def reset_cwd():
    cwd = os.getcwd()
    yield
    os.chdir(cwd)


@pytest.fixture
def mock_gcengine():
    mock_ex = mock.Mock(
        status_polling_interval=5,
        launch_cmd="foo-bar",
        interchange_launch_cmd="foo-bar",
    )

    with mock.patch.object(GlobusComputeEngine, "_ExecutorClass", mock.Mock):
        gce = GlobusComputeEngine(executor=mock_ex)
        yield gce
        gce.shutdown()


@pytest.mark.parametrize(
    "engine",
    (GlobusComputeEngine(address="::1"), ThreadPoolEngine(), ProcessPoolEngine()),
)
def test_set_working_dir_default(engine, tmp_path):
    """Verify that working dir is set to tasks dir in the run_dir by default for all
    engines
    """
    engine.set_working_dir(tmp_path)
    assert engine.working_dir == os.path.join(tmp_path, "tasks_working_dir")
    assert os.path.isabs(engine.working_dir) is True
    engine.shutdown()


@pytest.mark.parametrize(
    "engine",
    (GlobusComputeEngine(address="::1"), ThreadPoolEngine(), ProcessPoolEngine()),
)
def test_set_working_dir_called(engine, tmp_path, endpoint_uuid):
    """Verify that set_working_dir is called when engine.start() is called"""
    engine.executor = mock.Mock()
    engine.executor.status_polling_interval = 0
    engine.set_working_dir = mock.Mock(spec=engine.set_working_dir)

    engine.start(endpoint_id=endpoint_uuid, run_dir=tmp_path)
    assert engine.set_working_dir.called
    engine.shutdown()


@pytest.mark.parametrize(
    "engine",
    (GlobusComputeEngine(address="::1"), ThreadPoolEngine(), ProcessPoolEngine()),
)
def test_set_working_dir_relative(engine, tmp_path):
    """Working_dir should be absolute and set relative to the endpoint run_dir"""
    os.chdir(tmp_path)
    engine.working_dir = "tasks_working_dir"
    engine.set_working_dir(run_dir="foo")
    engine.shutdown()
    assert engine.working_dir == os.path.join(tmp_path, "foo", "tasks_working_dir")
    assert os.path.isabs(engine.working_dir) is True


def test_default_working_dir(tmp_path, mock_gcengine):
    """Test working_dir relative to run_dir"""
    mock_gcengine.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)
    assert mock_gcengine.run_dir == mock_gcengine.executor.run_dir
    assert mock_gcengine.working_dir == os.path.join(tmp_path, "tasks_working_dir")


def test_relative_working_dir(tmp_path, mock_gcengine):
    """Test working_dir relative to run_dir"""
    mock_gcengine.working_dir = "relative_path"
    mock_gcengine.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)
    assert mock_gcengine.run_dir == mock_gcengine.executor.run_dir
    assert mock_gcengine.working_dir == os.path.join(tmp_path, "relative_path")


def test_absolute_working_dir(tmp_path, mock_gcengine):
    """Test absolute path for working_dir"""
    mock_gcengine.working_dir = "/absolute/path"
    mock_gcengine.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)
    assert mock_gcengine.run_dir == mock_gcengine.executor.run_dir
    assert mock_gcengine.working_dir == "/absolute/path"


def test_submit_pass(tmp_path, task_uuid, mock_gcengine):
    """Test absolute path for working_dir"""
    mock_gcengine.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)

    mock_gcengine.submit(
        task_f=GCFuture(gc_task_id=task_uuid),
        packed_task=b"PACKED_TASK",
        resource_specification={},
    )

    mock_gcengine.executor.submit.assert_called()
    flag = False
    for call in mock_gcengine.executor.submit.call_args_list:
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
