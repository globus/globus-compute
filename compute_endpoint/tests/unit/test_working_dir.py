import os
import uuid
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines.globus_compute import GlobusComputeEngine
from globus_compute_endpoint.engines.helper import execute_task
from globus_compute_sdk.serialize import ComputeSerializer
from parsl.executors import HighThroughputExecutor
from tests.utils import ez_pack_function


@pytest.fixture()
def reset_cwd():
    cwd = os.getcwd()
    yield
    os.chdir(cwd)


def get_cwd():
    import os

    return os.getcwd()


def test_default_working_dir(tmp_path):
    """Verify that working dir is set to tasks dir in the run_dir by default"""
    gce = GlobusComputeEngine(
        address="127.0.0.1",
    )
    gce.executor.start = mock.MagicMock(spec=HighThroughputExecutor.start)
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)
    assert gce.executor.run_dir == tmp_path
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


def test_submit_pass(tmp_path):
    """Test absolute path for working_dir"""
    gce = GlobusComputeEngine(
        address="127.0.0.1",
    )
    gce.executor.start = mock.Mock(spec=HighThroughputExecutor.start)
    gce.executor.submit = mock.Mock(spec=HighThroughputExecutor.submit)
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)

    gce.submit(
        task_id=uuid.uuid4(),
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


def test_execute_task_working_dir(tmp_path, reset_cwd):

    task_id = uuid.uuid4()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, get_cwd, (), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    assert os.getcwd() != tmp_path.name

    packed_result = execute_task(
        task_id,
        task_message,
        uuid.uuid4(),
        run_dir=tmp_path,
    )

    message = messagepack.unpack(packed_result)
    assert message.task_id == task_id
    assert message.data
    result = serializer.deserialize(message.data)
    assert result == tmp_path.__fspath__()
    assert os.getcwd() == tmp_path.__fspath__()


def test_non_existent_relative_working_dir(tmp_path, reset_cwd):
    """This tests for execute_task creating a non-existent working dir
    when a relative path is specified to the CWD"""

    task_id = uuid.uuid4()
    os.chdir(tmp_path)
    target_dir = f"{uuid.uuid4()}"
    abs_target_dir = os.path.abspath(target_dir)
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, get_cwd, (), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    assert os.getcwd() != target_dir

    packed_result = execute_task(
        task_id,
        task_message,
        uuid.uuid4(),
        run_dir=target_dir,
    )

    message = messagepack.unpack(packed_result)
    assert message.task_id == task_id
    assert message.data
    result = serializer.deserialize(message.data)

    assert result == abs_target_dir


def test_sandbox(tmp_path, reset_cwd):

    task_id = uuid.uuid4()
    os.chdir(tmp_path)
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, get_cwd, (), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    assert os.getcwd() != tmp_path

    packed_result = execute_task(
        task_id,
        task_message,
        uuid.uuid4(),
        run_dir=tmp_path,
        run_in_sandbox=True,
    )

    message = messagepack.unpack(packed_result)
    assert message.task_id == task_id
    assert message.data
    result = serializer.deserialize(message.data)

    assert result == os.path.join(tmp_path, str(task_id))
