import logging
import shlex
from types import SimpleNamespace
from unittest import mock

import pytest
from globus_compute_sdk.sdk.hardware_report import (
    _run_command,
    cpu_info,
    mem_info,
    run_hardware_report,
    swap_info,
)

_MOCK_BASE = "globus_compute_sdk.sdk.hardware_report."
_NON_PORTABLE_COMMANDS = [cpu_info(), "lshw", "nvidia-smi"]


@pytest.mark.parametrize("missing_command", _NON_PORTABLE_COMMANDS)
def test_excludes_missing_commands(mocker, missing_command):
    mocker.patch(
        f"{_MOCK_BASE}_run_command", lambda s: None if missing_command in s else s
    )

    env = "test env"
    report = run_hardware_report(env)

    for command in _NON_PORTABLE_COMMANDS:
        if command == missing_command:
            assert command not in report
        else:
            assert command in report
        assert f"== {env} ==" in report


@pytest.mark.parametrize("missing_command", _NON_PORTABLE_COMMANDS)
def test_run_command_handles_missing_commands(mocker, caplog, missing_command):
    missing_cmd_arg0 = shlex.split(missing_command)[0]
    mocker.patch(f"{_MOCK_BASE}shutil.which", lambda s: s != missing_cmd_arg0)
    mocker.patch(
        f"{_MOCK_BASE}subprocess.run", lambda s, **_: SimpleNamespace(stdout=s[0])
    )

    for command in _NON_PORTABLE_COMMANDS:
        with caplog.at_level(logging.INFO):
            caplog.clear()
            output = _run_command(command)

        command_arg0 = shlex.split(command)[0]
        if command == missing_command:
            assert output is None
            assert f"{command_arg0} was not found in the PATH" in caplog.text
        else:
            assert output == command_arg0
            assert f"{command_arg0} was not found in the PATH" not in caplog.text


@pytest.mark.parametrize("psutil_using_func", (mem_info, swap_info))
def test_missing_psutil_handled_gracefully(psutil_using_func):
    ps_mock = mock.Mock()
    with mock.patch.dict("sys.modules", {"psutil": ps_mock}):
        with mock.patch(f"{_MOCK_BASE}_format_psmem", side_effect=MemoryError):
            with pytest.raises(MemoryError):
                psutil_using_func()

    with mock.patch.dict("sys.modules", {"psutil": None}):
        with mock.patch("builtins.__import__", side_effect=ImportError):
            r = psutil_using_func()
    assert "`psutil` not installed" in r
