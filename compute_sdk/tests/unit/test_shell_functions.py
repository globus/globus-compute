import json
import os
import random
import uuid

import pytest
from globus_compute_sdk.sdk.shell_function import ShellFunction, ShellResult


@pytest.mark.parametrize("rc", (0, random.randint(1, 256)))
@pytest.mark.parametrize("exc", (None, "Some Exception Text"))
@pytest.mark.parametrize("res_len", (1024, 1025))
def test_shell_result_str(rc, exc, res_len):
    sout = " sout\n"
    sout = (sout * (1 + res_len // len(sout)))[:res_len]
    serr = sout.replace("sout", "serr")
    assert len(sout) == res_len, "Verify test setup"

    s = str(ShellResult("cmd", sout, serr, rc, exc))

    exp_sout = "\n".join(sout.strip()[-1024:].splitlines()[-10:])

    assert f"exit code: {rc}\n" in s
    assert "   stdout:\n" in s
    assert exp_sout in s
    if exp_sout != sout:
        assert "truncated; see .stdout" in s

    if rc == 0:
        assert "stderr:" not in s
        assert "exception:" not in s
    else:
        exp_serr = "\n".join(serr.strip()[-1024:].splitlines()[-10:])

        assert "   stderr:\n" in s
        assert exp_serr in s
        if exp_serr != serr:
            assert "truncated; see .stderr" in s

        if exc:
            assert f"\n\nexception: {exc}" in s


@pytest.mark.parametrize("excname", (None, "Some Exception name"))
def test_shell_result_repr(excname):
    cmd = "some command more than 80 chars: " + "a" * 80
    sr = ShellResult(
        cmd,
        stdout="some stdout",
        stderr="some stderr",
        returncode=43,
        exception_name=excname,
    )
    r = repr(sr)

    assert r.startswith(ShellResult.__name__)
    assert f"returncode={sr.returncode}" in r
    assert f"cmd={cmd!r}" in r
    assert f"stdout={sr.stdout!r}" in r
    assert f"stderr={sr.stderr!r}" in r

    if excname is not None:
        assert f"exception_name={excname!r}" in r


@pytest.mark.parametrize("name", (None, "somename"))
@pytest.mark.parametrize("walltime", (None, 15.7))
@pytest.mark.parametrize("sout", (None, "/some / path"))
@pytest.mark.parametrize("serr", (None, "/other/path/"))
@pytest.mark.parametrize("lines", (None, 12345))
@pytest.mark.parametrize("cmd_len", (10, 1024, 1025))
def test_shell_function_str(name, walltime, sout, serr, lines, cmd_len):
    cmd = "some script's line\n"
    cmd = (cmd * (1 + cmd_len // len(cmd)))[:cmd_len]
    assert len(cmd) == cmd_len, "Verify test setup"

    k = {
        "name": name,
        "walltime": walltime,
        "stdout": sout,
        "stderr": serr,
    }
    if lines:
        k["snippet_lines"] = lines

    sf = ShellFunction(cmd, **k)
    s = str(sf)
    name = name or ShellFunction.__name__

    assert s.startswith(f"{name}\n")
    if walltime is not None:
        assert f"\n wall time: {walltime}" in s
    if sout is not None:
        assert f"\n    stdout: {sout!r}" in s
    if serr is not None:
        assert f"\n    stderr: {serr!r}" in s

    exp_lines = f"\n     lines: {lines or 1000}"
    assert exp_lines in s

    command_header = "   command: "
    exp_cmd = "\n".join(cmd.strip()[:1024].splitlines()[:10])
    if "\n" not in exp_cmd:
        assert f"\n{command_header}{exp_cmd}" in s
    else:
        indent = "\n" + " " * len(command_header)
        exp_cmd = exp_cmd.strip().replace("\n", indent)
        assert f"\n   command: -----{indent}{exp_cmd}" in s
        assert "truncated; see .cmd for full script" in s


@pytest.mark.parametrize("name", (None, "somename"))
@pytest.mark.parametrize("walltime", (None, 0.5, 15.7))
@pytest.mark.parametrize("sout", (None, "/some / path"))
@pytest.mark.parametrize("serr", (None, "/other/path/"))
@pytest.mark.parametrize("lines", (None, 918273))
def test_shell_function_repr(name, walltime, sout, serr, lines):
    cmd = "some command -with --arguments just because"
    k = {
        "name": name,
        "walltime": walltime,
        "stdout": sout,
        "stderr": serr,
    }
    if lines:
        k["snippet_lines"] = lines

    sf = ShellFunction(cmd, **k)
    s = repr(sf)

    assert s.startswith(ShellFunction.__name__)
    if walltime is not None:
        assert f"walltime={walltime!r}" in s
    if sout is not None:
        assert f"stdout={sout!r}" in s
    if serr is not None:
        assert f"stderr={serr!r}" in s

    if name is not None:
        assert f"name={name!r}" in s

    exp_lines = f"snippet_lines={lines or 1000}"
    assert exp_lines in s
    assert f"cmd={cmd!r}" in s


def test_shell_function_no_stdfile(run_in_tmp_dir):
    """Confirm that stdout/err snippet is reported after successful run"""

    os.environ["GC_TASK_SANDBOX_DIR"] = f"{run_in_tmp_dir}/{str(uuid.uuid4())}"
    bf = ShellFunction("echo 'Hello'")
    bf_result = bf()

    assert isinstance(bf_result, ShellResult)

    assert bf.__name__ == type(bf).__name__
    assert "Hello" in bf_result.stdout
    assert not bf_result.stderr
    assert "exit code: 0" in str(bf_result)


def test_shell_function_with_stdfile(run_in_tmp_dir):
    """Confirm that stdout/err snippet to relative stdout and abs err paths"""

    os.environ["GC_TASK_SANDBOX_DIR"] = f"{run_in_tmp_dir}/{str(uuid.uuid4())}"
    bf = ShellFunction(
        "echo 'Hello'", stdout="foo.out", stderr=f"{run_in_tmp_dir}/foo.err"
    )
    bf_result = bf()

    assert isinstance(bf_result, ShellResult)

    assert "Hello" in bf_result.stdout
    assert not bf_result.stderr
    assert "exit code: 0" in str(bf_result)
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


def test_shell_function_return_dict(run_in_tmp_dir, monkeypatch):
    monkeypatch.setenv("GC_TASK_SANDBOX_DIR", f"{run_in_tmp_dir}/{str(uuid.uuid4())}")

    output = "Hello"
    func = ShellFunction(f"echo '{output}'", return_dict=True)
    res = func()

    assert isinstance(res, dict)
    assert output in res["stdout"]
    try:
        s = json.dumps(res)
        json.loads(s)
    except (TypeError, ValueError) as e:
        pytest.fail(f"Result is not JSON-serializable: {e}")


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


@pytest.mark.parametrize("fn_name", ["for", "with", "break", " abc", "2be"])
def test_bad_shell_function_name(fn_name):
    """BashFunction should raise a ValueError on invalid function names"""
    with pytest.raises(ValueError):
        ShellFunction("pwd", name=fn_name)


@pytest.mark.parametrize("fn_name", ["Hello", "World", "My_Shell_Function", "École"])
def test_shell_function_name(fn_name):
    """BashFunction should have name set at init"""
    sf = ShellFunction("pwd", name=fn_name)
    assert sf.__name__ == fn_name


def test_shell_function_instance_naming():
    """Setting ShellFunction instance names should not mutate the class name"""
    sf_a = ShellFunction("pwd", name="A")
    sf_b = ShellFunction("pwd", name="B")

    assert sf_a.__name__ == "A"
    assert sf_b.__name__ == "B"
    assert ShellFunction.__name__ == "ShellFunction"
