from __future__ import annotations

import json
import os
import pathlib
import shlex
import uuid
from unittest import mock

import pytest
import yaml
from click import ClickException
from click.testing import CliRunner
from globus_compute_endpoint.cli import app, init_config_dir
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.endpoint.config.utils import load_config_yaml


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
        ep_template = ep_dir / "config_user.yaml"
        ep_config.write_text(
            """
display_name: null
engine:
    type: HighThroughputEngine
    provider:
        type: LocalProvider
        init_blocks: 1
        min_blocks: 0
        max_blocks: 1
            """
        )
        ep_template.write_text(
            """
heartbeat_period: {{ heartbeat }}
engine:
    type: HighThroughputEngine
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


@pytest.mark.parametrize("cli_cmd", ["start", "configure", "stop"])
def test_endpoint_uuid_name_not_supported(run_line, cli_cmd):
    ep_uuid_name = uuid.uuid4()
    res = run_line(f"{cli_cmd} {ep_uuid_name}", assert_exit_code=2)
    assert "UUID" in res.stderr and "not currently supported" in res.stderr


@pytest.mark.parametrize(
    "stdin_data",
    [
        (False, "..."),
        (False, "()"),
        (False, json.dumps([1, 2, 3])),
        (False, json.dumps("abc")),
        (True, "{}"),
        (True, json.dumps({"amqp_creds": {}})),
        (True, json.dumps({"config": "myconfig"})),
        (True, json.dumps({"amqp_creds": {}, "config": ""})),
        (True, json.dumps({"amqp_creds": {"a": 1}, "config": "myconfig"})),
        (True, json.dumps({"amqp_creds": {}, "config": "myconfig"})),
    ],
)
def test_start_ep_reads_stdin(
    mocker, run_line, mock_cli_state, make_endpoint_dir, stdin_data
):
    data_is_valid, data = stdin_data

    mock_load_conf = mocker.patch("globus_compute_endpoint.cli.load_config_yaml")
    mock_load_conf.return_value = Config()
    mock_get_config = mocker.patch("globus_compute_endpoint.cli.get_config")
    mock_get_config.return_value = Config()

    mock_log = mocker.patch("globus_compute_endpoint.cli.log")
    mock_sys = mocker.patch("globus_compute_endpoint.cli.sys")
    mock_sys.stdin.closed = False
    mock_sys.stdin.isatty.return_value = False
    mock_sys.stdin.read.return_value = data

    make_endpoint_dir("foo")

    run_line("start foo")
    mock_ep, _ = mock_cli_state
    assert mock_ep.start_endpoint.called
    reg_info_found = mock_ep.start_endpoint.call_args[0][5]

    if data_is_valid:
        data_dict = json.loads(data)
        reg_info = data_dict.get("amqp_creds", {})
        config_str = data_dict.get("config", None)

        assert reg_info_found == reg_info
        if config_str:
            config_str_found = mock_load_conf.call_args[0][0]
            assert config_str_found == config_str

    else:
        assert mock_get_config.called
        assert mock_log.debug.called
        a, k = mock_log.debug.call_args
        assert "Invalid info on stdin" in a[0]
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

    with open(conf) as f:
        conf_dict = yaml.safe_load(f)

    assert conf_dict["display_name"] == display_name


@pytest.mark.parametrize("display_name", [None, "None"])
def test_config_yaml_display_none(
    run_line, mock_command_ensure, make_endpoint_dir, display_name
):
    ep_name = "test_display_none"

    conf = mock_command_ensure.endpoint_config_dir / ep_name / "config.yaml"

    config_cmd = f"configure {ep_name}"
    if display_name is not None:
        config_cmd += f" --display-name {display_name}"
    run_line(config_cmd)

    with open(conf) as f:
        conf_dict = load_config_yaml(f)

    assert conf_dict.display_name is None


def test_start_ep_incorrect_config_yaml(run_line, mock_cli_state, make_endpoint_dir):
    make_endpoint_dir("foo")
    mock_ep, mock_state = mock_cli_state
    conf = mock_state.endpoint_config_dir / "foo" / "config.yaml"

    conf.write_text("asdf")
    res = run_line("start foo", assert_exit_code=1)
    assert "Invalid config syntax" in res.stderr

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


@mock.patch("globus_compute_endpoint.endpoint.config.utils._load_config_py")
def test_start_ep_config_py_override(
    read_config, run_line, mock_cli_state, make_endpoint_dir
):
    make_endpoint_dir("foo")
    mock_ep, mock_state = mock_cli_state
    conf_py = mock_state.endpoint_config_dir / "foo" / "config.py"
    conf_py.write_text(
        """
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.engines import HighThroughputEngine
from parsl.providers import LocalProvider

config = Config(
    display_name=None,
    executors=[
        HighThroughputEngine(
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


@mock.patch("globus_compute_endpoint.endpoint.config.utils._load_config_py")
def test_delete_endpoint(read_config, run_line, mock_cli_state):
    run_line("delete foo --yes")
    mock_ep, _ = mock_cli_state
    mock_ep.delete_endpoint.assert_called_once()


@pytest.mark.parametrize("die_with_parent", [True, False])
@mock.patch("globus_compute_endpoint.cli.get_config")
def test_die_with_parent_detached(
    mock_get_config, run_line, mock_cli_state, die_with_parent
):
    config = Config()
    mock_get_config.return_value = config

    if die_with_parent:
        run_line("start foo --die-with-parent")
        assert not config.detach_endpoint
    else:
        run_line("start foo")
        assert config.detach_endpoint
