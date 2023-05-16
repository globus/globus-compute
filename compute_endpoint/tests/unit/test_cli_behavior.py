from __future__ import annotations

import json
import os
import pathlib
import shlex
from unittest import mock

import pytest
import yaml
from click import ClickException
from click.testing import CliRunner
from globus_compute_endpoint.cli import app, init_config_dir


@pytest.fixture
def funcx_dir_path(tmp_path):
    yield tmp_path / "funcx-dir"


@pytest.fixture
def mock_command_ensure(funcx_dir_path):
    with mock.patch("globus_compute_endpoint.cli.CommandState.ensure") as m_state:
        mock_state = mock.Mock()
        mock_state.endpoint_config_dir = funcx_dir_path
        m_state.return_value = mock_state

        yield mock_state


@pytest.fixture
def mock_cli_state(funcx_dir_path, mock_command_ensure):
    with mock.patch("globus_compute_endpoint.cli.Endpoint") as mock_ep:
        mock_ep.return_value = mock_ep
        yield mock_ep, mock_command_ensure


@pytest.fixture
def make_endpoint_dir(mock_command_ensure):
    def func(name):
        ep_dir = mock_command_ensure.endpoint_config_dir / name
        ep_dir.mkdir(parents=True, exist_ok=True)
        ep_config = ep_dir / "config.yaml"
        ep_config.write_text(  # Default config for testing
            """
display_name: None
executor:
    provider:
        type: LocalProvider
        init_blocks: 1
        min_blocks: 0
        max_blocks: 1
            """
        )

    return func


@pytest.fixture
def cli_runner():
    return CliRunner(mix_stderr=False)


@pytest.fixture
def run_line(cli_runner):
    def func(argline, *, assert_exit_code: int | None = 0):
        args = shlex.split(argline) if isinstance(argline, str) else argline

        result = cli_runner.invoke(app, args)
        if assert_exit_code is not None:
            assert result.exit_code == assert_exit_code, (result.stdout, result.stderr)
        return result

    return func


@pytest.mark.parametrize("dir_exists", [True, False])
@pytest.mark.parametrize("user_dir", ["/my/dir", None, ""])
def test_init_config_dir(fs, dir_exists, user_dir):
    config_dirname = pathlib.Path.home() / ".globus_compute"

    if dir_exists:
        fs.create_dir(config_dirname)

    if user_dir is not None:
        config_dirname = pathlib.Path(user_dir)
        with mock.patch.dict(
            os.environ, {"GLOBUS_COMPUTE_USER_DIR": str(config_dirname)}
        ):
            dirname = init_config_dir()
    else:
        dirname = init_config_dir()

    assert dirname == config_dirname


def test_init_config_dir_file_conflict(fs):
    filename = pathlib.Path.home() / ".globus_compute"
    fs.create_file(filename)

    with pytest.raises(ClickException) as exc:
        init_config_dir()

    assert "Error creating directory" in str(exc)


def test_init_config_dir_permission_error(fs):
    parent_dirname = pathlib.Path("/parent/dir/")
    config_dirname = parent_dirname / "config"

    fs.create_dir(parent_dirname)
    os.chmod(parent_dirname, 0o000)

    with pytest.raises(ClickException) as exc:
        with mock.patch.dict(
            os.environ, {"GLOBUS_COMPUTE_USER_DIR": str(config_dirname)}
        ):
            init_config_dir()

    assert "Permission denied" in str(exc)


def test_start_ep_corrupt(run_line, mock_cli_state, make_endpoint_dir):
    make_endpoint_dir("foo")
    mock_ep, mock_state = mock_cli_state
    conf = mock_state.endpoint_config_dir / "foo" / "config.yaml"
    conf.unlink()
    res = run_line("start foo", assert_exit_code=1)
    assert "corrupted?" in res.stderr


def test_start_endpoint_no_such_ep(run_line, mock_cli_state):
    res = run_line("start foo", assert_exit_code=1)
    mock_ep, _ = mock_cli_state
    mock_ep.start_endpoint.assert_not_called()
    assert "Endpoint 'foo' is not configured" in res.stderr


def test_start_endpoint_existing_ep(run_line, mock_cli_state, make_endpoint_dir):
    make_endpoint_dir("foo")
    run_line("start foo")
    mock_ep, _ = mock_cli_state
    mock_ep.start_endpoint.assert_called_once()


@pytest.mark.parametrize(
    "reg_data",
    [
        (False, "..."),
        (False, "()"),
        (False, json.dumps([1, 2, 3])),
        (False, json.dumps("abc")),
        (True, json.dumps({"a": 1})),
        (True, json.dumps({})),
    ],
)
def test_start_ep_reads_stdin(
    mocker, run_line, mock_cli_state, make_endpoint_dir, reg_data
):
    data_is_valid, reg_info = reg_data

    mock_log = mocker.patch("globus_compute_endpoint.cli.log")
    mock_sys = mocker.patch("globus_compute_endpoint.cli.sys")
    mock_sys.stdin.closed = False
    mock_sys.stdin.isatty.return_value = False
    mock_sys.stdin.read.return_value = reg_info

    make_endpoint_dir("foo")

    run_line("start foo")
    mock_ep, _ = mock_cli_state
    assert mock_ep.start_endpoint.called
    reg_info_found = mock_ep.start_endpoint.call_args[0][5]

    if data_is_valid:
        assert reg_info_found == json.loads(reg_info)

    else:
        assert mock_log.debug.called
        a, k = mock_log.debug.call_args
        assert "Invalid registration info on stdin" in a[0]
        assert reg_info_found == {}


@mock.patch("globus_compute_endpoint.cli.get_config")
def test_stop_endpoint(get_config, run_line, mock_cli_state, make_endpoint_dir):
    run_line("stop foo")
    mock_ep, _ = mock_cli_state
    mock_ep.stop_endpoint.assert_called_once()


def test_restart_endpoint_does_start_and_stop(
    run_line, mock_cli_state, make_endpoint_dir
):
    make_endpoint_dir("foo")
    run_line("restart foo")

    mock_ep, _ = mock_cli_state
    mock_ep.stop_endpoint.assert_called_once()
    mock_ep.start_endpoint.assert_called_once()


def test_configure_validates_name(mock_command_ensure, run_line):
    compute_dir = mock_command_ensure.endpoint_config_dir
    compute_dir.mkdir(parents=True, exist_ok=True)

    run_line("configure ValidName")
    run_line("configure 'Invalid name with spaces'", assert_exit_code=1)


@pytest.mark.parametrize(
    "display_test",
    [
        ["ep0", None],
        ["ep1", "ep/ .1"],
        ["ep2", "abc 😎 /.great"],
    ],
)
def test_start_ep_display_name_in_config(
    run_line, mock_command_ensure, make_endpoint_dir, display_test
):
    ep_name, display_name = display_test

    conf = mock_command_ensure.endpoint_config_dir / ep_name / "config.yaml"
    configure_arg = ""
    if display_name is not None:
        configure_arg = f" --display-name '{display_name}'"
    run_line(f"configure {ep_name}{configure_arg}")

    dn_str = f"{display_name}" if display_name is not None else "None"

    with open(conf) as f:
        conf_dict = yaml.safe_load(f)

    assert conf_dict["display_name"] == dn_str


def test_start_ep_incorrect_config_yaml(run_line, mock_cli_state, make_endpoint_dir):
    make_endpoint_dir("foo")
    mock_ep, mock_state = mock_cli_state
    conf = mock_state.endpoint_config_dir / "foo" / "config.yaml"

    conf.write_text("asdf")
    res = run_line("start foo", assert_exit_code=1)
    assert "Invalid syntax" in res.stderr

    # `coverage` demands a valid syntax file.  FBOW, then, the ordering and
    # commingling of these two tests is intentional.  Bit of a meta problem ...
    conf.unlink()
    conf.write_text("asdf: asdf")
    res = run_line("start foo", assert_exit_code=1)
    assert "field required" in res.stderr


def test_start_ep_incorrect_config_py(run_line, mock_cli_state, make_endpoint_dir):
    make_endpoint_dir("foo")
    mock_ep, mock_state = mock_cli_state
    conf = mock_state.endpoint_config_dir / "foo" / "config.py"

    conf.write_text("asa asd df = 5")  # fail the import
    with mock.patch("globus_compute_endpoint.endpoint.config.utils.log") as mock_log:
        res = run_line("start foo", assert_exit_code=1)
        assert "might be out of date" in mock_log.exception.call_args[0][0]
    assert isinstance(res.exception, SyntaxError)

    # `coverage` demands a valid syntax file.  FBOW, then, the ordering and
    # commingling of these two tests is intentional.  Bit of a meta problem ...
    conf.unlink()
    conf.write_text("asdf = 5")  # syntactically correct
    res = run_line("start foo", assert_exit_code=1)
    assert "modified incorrectly?" in res.stderr


@mock.patch("globus_compute_endpoint.endpoint.config.utils.read_config_py")
def test_start_ep_config_py_override(
    read_config, run_line, mock_cli_state, make_endpoint_dir
):
    make_endpoint_dir("foo")
    mock_ep, mock_state = mock_cli_state
    conf_py = mock_state.endpoint_config_dir / "foo" / "config.py"
    conf_py.write_text(
        """
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

config = Config(
    display_name=None,
    executors=[
        HighThroughputExecutor(
            provider=LocalProvider(
                init_blocks=1,
                min_blocks=0,
                max_blocks=1,
            ),
        )
    ],
)
        """
    )

    run_line("start foo", assert_exit_code=1)
    read_config.assert_called_once()


@mock.patch("globus_compute_endpoint.endpoint.config.utils.read_config_py")
def test_delete_endpoint(read_config, run_line, mock_cli_state):
    run_line("delete foo --yes")
    mock_ep, _ = mock_cli_state
    mock_ep.delete_endpoint.assert_called_once()
