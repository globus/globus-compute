import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GlobusComputeEngine
from globus_compute_sdk.sdk.shell_function import ShellFunction


def test_shell_function(engine_runner, tmp_path, task_uuid, serde, ez_pack_task):
    """Test running ShellFunction with GCE: Happy path"""
    engine = engine_runner(GlobusComputeEngine)
    shell_func = ShellFunction("pwd")
    task_bytes = ez_pack_task(shell_func)
    future = engine.submit(task_uuid, task_bytes, resource_specification={})

    packed_result = future.result()
    result = messagepack.unpack(packed_result)

    assert result.task_id == task_uuid
    assert result.error_details is None
    result_obj = serde.deserialize(result.data)

    assert "pwd" == result_obj.cmd
    assert result_obj.returncode == 0
    assert tmp_path.name in result_obj.stdout


@pytest.mark.parametrize(
    "cmd, error_str, returncode",
    [
        ("sleep 5", "", 124),
        ("cat /NONEXISTENT", "No such file or directory", 1),
        ("echo 'very bad' 1>&2; exit 3", "very bad", 3),
        ("fake_command", "command not found", 127),
        ("touch foo; ./foo", "Permission denied", 126),
    ],
)
def test_fail_shell_function(
    engine_runner, cmd, error_str, returncode, serde, task_uuid, ez_pack_task
):
    """Test running ShellFunction with GCE: Failure path"""
    engine = engine_runner(GlobusComputeEngine, run_in_sandbox=True)
    shell_func = ShellFunction(cmd, walltime=0.1)
    task_bytes = ez_pack_task(shell_func)
    future = engine.submit(task_uuid, task_bytes, resource_specification={})

    packed_result = future.result()
    result = messagepack.unpack(packed_result)
    assert result.task_id == task_uuid
    assert not result.error_details

    result_obj = serde.deserialize(result.data)

    assert error_str in result_obj.stderr
    assert result_obj.returncode == returncode


def test_no_sandbox(engine_runner, task_uuid, serde, ez_pack_task):
    """Test running ShellFunction without sandbox"""
    engine = engine_runner(GlobusComputeEngine, run_in_sandbox=False)
    shell_func = ShellFunction("pwd")
    task_bytes = ez_pack_task(shell_func)
    future = engine.submit(task_uuid, task_bytes, resource_specification={})

    packed_result = future.result()
    result = messagepack.unpack(packed_result)
    assert result.task_id == task_uuid
    assert result.error_details is None
    result_obj = serde.deserialize(result.data)

    assert "pwd" == result_obj.cmd
    assert result_obj.returncode == 0
    assert (
        "WARNING: Task sandboxing will not work due to endpoint misconfiguration."
        in result_obj.stderr
    )
    assert f"{engine.run_dir}/tasks_working_dir" == result_obj.stdout.strip()
