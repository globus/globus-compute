import os
import uuid

import pytest
from globus_compute_sdk.sdk.shell_function import ShellFunction, ShellResult


def test_shell_function_no_stdfile(run_in_tmp_dir):
    """Confirm that stdout/err snippet is reported after successful run"""

    os.environ["GC_TASK_SANDBOX_DIR"] = f"{run_in_tmp_dir}/{str(uuid.uuid4())}"
    bf = ShellFunction("echo 'Hello'")
    bf_result = bf()

    assert isinstance(bf_result, ShellResult)

    assert bf.__name__ == "ShellFunction: echo 'Hello'"
    assert "Hello" in bf_result.stdout
    assert not bf_result.stderr
    assert "returned with exit status: 0" in str(bf_result)


def test_shell_function_with_stdfile(run_in_tmp_dir):
    """Confirm that stdout/err snippet to relative stdout and abs err paths"""

    os.environ["GC_TASK_SANDBOX_DIR"] = f"{run_in_tmp_dir}/{str(uuid.uuid4())}"
    bf = ShellFunction(
        "echo 'Hello'", stdout="foo.out", stderr=f"{run_in_tmp_dir}/foo.err"
    )
    bf_result = bf()

    assert isinstance(bf_result, ShellResult)

    assert bf.__name__ == "ShellFunction: echo 'Hello'"
    assert "Hello" in bf_result.stdout
    assert not bf_result.stderr
    assert "returned with exit status: 0" in str(bf_result)
    assert os.path.exists(f"{os.environ['GC_TASK_SANDBOX_DIR']}/foo.out")
    assert os.path.exists(f"{run_in_tmp_dir}/foo.err")


def test_repeated_invocation(run_in_tmp_dir):
    """Confirm that stdout/err snippet is isolated between repeated runs"""

    os.environ["GC_TASK_SANDBOX_DIR"] = f"{run_in_tmp_dir}/{str(uuid.uuid4())}"
    cmd = "pwd; echo 'Hello {target}'; echo 'Bye {target}' 1>&2"
    bf = ShellFunction(cmd)

    prev = "world_0"

    for i in range(1, 3):
        target = f"world_{i}"
        os.environ["GC_TASK_SANDBOX_DIR"] = f"{run_in_tmp_dir}/{str(uuid.uuid4())}"
        result = bf(target=target)

        assert f"Hello {target}" in result.stdout
        assert f"Bye {target}" == result.stderr.strip()
        assert result.cmd == cmd.format(target=target)

        assert prev not in result.stdout, "Output from previous invocation present"
        prev = target


def test_timeout(run_in_tmp_dir):
    """Test for timeout returning correct returncode"""
    os.environ["GC_TASK_SANDBOX_DIR"] = f"{run_in_tmp_dir}/{str(uuid.uuid4())}"
    bf = ShellFunction("sleep 5; echo Fail", walltime=0.1)

    result_obj = bf()
    assert isinstance(result_obj, ShellResult)
    assert result_obj.returncode == 124
    assert "Fail" not in result_obj.stdout


def test_snippet_capture_on_walltime_fail(run_in_tmp_dir):
    """Confirm that stdout/err snippet on walltime fail"""
    os.environ["GC_TASK_SANDBOX_DIR"] = f"{run_in_tmp_dir}/{str(uuid.uuid4())}"
    cmd = "echo 'Hello'; sleep 5"
    bf = ShellFunction(cmd, walltime=0.1)

    result = bf()

    assert result.returncode == 124
    assert "Hello" in result.stdout
    assert not result.stderr


def test_fail(run_in_tmp_dir):
    """Confirm failed cmd behavior"""
    os.environ["GC_TASK_SANDBOX_DIR"] = f"{run_in_tmp_dir}/{str(uuid.uuid4())}"
    cmd = "echo 'hi'; cat /non_existent"
    bf = ShellFunction(cmd, walltime=0.1)

    result = bf()

    assert result.returncode == 1
    assert "hi" in result.stdout
    assert result.cmd == cmd
    assert "No such file or directory" in result.stderr


def test_bad_run_dir():
    """Confirm that error on bad sandbox dir"""
    os.environ["GC_TASK_SANDBOX_DIR"] = "/NONEXISTENT"
    bf = ShellFunction("pwd")

    with pytest.raises(OSError):
        bf()


def test_sandbox_warning_with_missing_env_vars(run_in_tmp_dir):
    """Confirm that stderr brings back warning of sandboxing
    not working on older EPs"""
    os.environ.pop("GC_TASK_UUID", None)
    os.environ.pop("GC_TASK_SANDBOX_DIR", None)
    bf = ShellFunction("pwd")

    result = bf()
    assert "WARNING: Task sandboxing will not work" in result.stderr


def test_skip_redundant_sandbox(run_in_tmp_dir):
    """Confirm that sandbox is not created if executor is set to create sandbox"""
    task_id = str(uuid.uuid4())

    os.environ["GC_TASK_SANDBOX_DIR"] = run_in_tmp_dir.name
    os.environ["GC_TASK_UUID"] = task_id
    bf = ShellFunction("pwd")

    result = bf()
    assert os.path.join(run_in_tmp_dir, task_id) not in result.stdout
    assert run_in_tmp_dir.name in result.stdout


def test_backward_compat(run_in_tmp_dir):
    """Confirm stdout/err paths on endpoints that do not have
    sandbox or task UUID exported"""
    task_id = str(uuid.uuid4())

    os.environ.pop("GC_TASK_SANDBOX_DIR", None)
    os.environ.pop("GC_TASK_UUID", None)
    bf = ShellFunction("pwd")

    result = bf()
    assert os.path.join(run_in_tmp_dir, task_id) not in result.stdout
    assert run_in_tmp_dir.name in result.stdout
    for std_file in os.listdir(run_in_tmp_dir):
        assert len(std_file.split(".")) == 3
        assert std_file.split(".")[-1] in ["stdout", "stderr"]


def test_stdout_naming(run_in_tmp_dir):
    """Confirm stdout/err paths are prefixed with task_uuid"""
    task_id = str(uuid.uuid4())

    os.environ.pop("GC_TASK_SANDBOX_DIR", None)
    os.environ["GC_TASK_UUID"] = task_id
    bf = ShellFunction("pwd")

    result = bf()
    assert os.path.join(run_in_tmp_dir, task_id) not in result.stdout
    assert run_in_tmp_dir.name in result.stdout
    for std_file in os.listdir(run_in_tmp_dir):

        assert len(std_file.split(".")) == 3
        assert std_file.split(".")[-1] in ["stdout", "stderr"]
        assert std_file.split(".")[0] == task_id


def test_bad_walltime(run_in_tmp_dir):
    """BashFunction should raise an error on negative walltime"""
    task_id = str(uuid.uuid4())

    os.environ.pop("GC_TASK_SANDBOX_DIR", None)
    os.environ["GC_TASK_UUID"] = task_id
    with pytest.raises(AssertionError):
        ShellFunction("pwd", walltime=-1)


@pytest.mark.parametrize(
    "shell_func, name",
    [
        (ShellFunction(cmd="foo", name="Foo"), "Foo"),
        (ShellFunction(cmd="bar"), "ShellFunction: bar"),
        (ShellFunction(cmd="bar\nbar"), "ShellFunction: bar"),
        (
            ShellFunction(cmd="10longword_20longword_30longword_40longword_50longword"),
            "ShellFunction: 10longword_20longword_30longword",
        ),
    ],
)
def test_shell_func_name(shell_func: ShellFunction, name: str):
    """Check if ShellFunction names are populated"""
    assert shell_func.__name__ == name
