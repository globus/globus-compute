import os
import uuid

import pytest
from globus_compute_sdk.sdk.bash_function import BashFunction, BashResult


@pytest.fixture
def run_in_tmp_dir(tmp_path):
    os.chdir(tmp_path)
    return tmp_path


def test_bash_function_no_stdfile(run_in_tmp_dir):
    """Confirm that stdout/err snippet is reported after successful run"""

    bf = BashFunction("echo 'Hello'")
    bf_result = bf()

    assert isinstance(bf_result, BashResult)

    assert "Hello" in bf_result.stdout
    assert not bf_result.stderr
    assert "returned with exit status: 0" in str(bf_result)


def test_repeated_invocation(run_in_tmp_dir):
    """Confirm that stdout/err snippet is reported after successful run"""

    os.environ["GC_TASK_ID"] = str(uuid.uuid4())
    cmd = "pwd; echo 'Hello {target}'; echo 'Bye {target}' 1>&2"
    bf = BashFunction(cmd, run_in_sandbox=True)

    prev = "world_0"

    for i in range(1, 3):
        target = f"world_{i}"
        result = bf(target=target, sandbox=True)

        assert f"Hello {target}" in result.stdout
        assert f"Bye {target}" == result.stderr.strip()
        assert result.cmd == cmd.format(target=target)

        assert prev not in result.stdout, "Output from previous invocation present"
        prev = target


def test_timeout(run_in_tmp_dir):
    """Test for timeout returning correct returncode"""
    bf = BashFunction("sleep 5; echo Fail", walltime=0.1)

    result_obj = bf()
    assert isinstance(result_obj, BashResult)
    assert result_obj.returncode == 124
    assert "Fail" not in result_obj.stdout


def test_snippet_capture_on_walltime_fail(run_in_tmp_dir):
    """Confirm that stdout/err snippet is reported after successful run"""
    cmd = "echo 'Hello'; sleep 5"
    bf = BashFunction(cmd, walltime=0.1)

    result = bf()

    assert result.returncode == 124
    assert "Hello" in result.stdout
    assert not result.stderr


def test_fail(run_in_tmp_dir):
    """Confirm failed cmd behavior"""
    cmd = "echo 'hi'; cat /non_existent"
    bf = BashFunction(cmd, walltime=0.1)

    result = bf()

    assert result.returncode == 1
    assert "hi" in result.stdout
    assert result.cmd == cmd
    assert "No such file or directory" in result.stderr


def test_default_run_dir(run_in_tmp_dir):
    """BF should run in CWD if a run_dir is not set"""
    bf = BashFunction("pwd")
    result = bf()

    assert str(os.getcwd()) in result.stdout
    assert not result.stderr


def test_run_dir(tmp_path):
    """BF should run in tmp_path and not in the PWD"""
    assert os.getcwd() != tmp_path

    bf = BashFunction("echo [$PWD]", rundir=tmp_path)
    result = bf()

    assert f"[{tmp_path}]" in result.stdout


def test_bad_run_dir():
    """Confirm that stdout/err snippet is reported after successful run"""
    bf = BashFunction("pwd", rundir="/NONEXISTENT")

    with pytest.raises(OSError):
        bf()


def test_sandbox_run_dir(run_in_tmp_dir):
    """Confirm that cmd is executed in a sandbox dir based on task_id"""
    task_id = str(uuid.uuid4())
    os.environ["GC_TASK_UUID"] = task_id
    bf = BashFunction("pwd", run_in_sandbox=True)

    result = bf()
    assert os.path.join(run_in_tmp_dir, task_id) == result.stdout.strip()


def test_no_sandbox_run_dir(run_in_tmp_dir):
    """Confirm that cmd is executed in a sandbox dir based on task_id"""
    task_id = str(uuid.uuid4())
    os.environ["GC_TASK_UUID"] = task_id
    os.environ.pop("GC_TASK_SANDBOX_DIR")
    bf = BashFunction("pwd", run_in_sandbox=False)

    result = bf()
    assert str(run_in_tmp_dir) == result.stdout.strip()


def test_skip_redundant_sandbox(run_in_tmp_dir):
    """Confirm that sandbox is not created if executor is set to create sandbox"""
    task_id = str(uuid.uuid4())

    os.environ["GC_TASK_SANDBOX_DIR"] = run_in_tmp_dir.name
    os.environ["GC_TASK_UUID"] = task_id
    bf = BashFunction("pwd", run_in_sandbox=True)

    result = bf()
    assert os.path.join(run_in_tmp_dir, task_id) not in result.stdout
    assert run_in_tmp_dir.name in result.stdout
