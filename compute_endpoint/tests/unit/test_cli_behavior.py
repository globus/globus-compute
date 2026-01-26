from __future__ import annotations

import inspect
import io
import json
import logging
import os
import pathlib
import random
import shlex
import sys
import time
import typing as t
import uuid
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import globus_sdk
import pytest
import yaml
from click import ClickException
from click import File as ClickFile
from click.testing import CliRunner
from globus_compute_endpoint import cli
from globus_compute_endpoint.cli import (
    _AUTH_POLICY_DEFAULT_DESC,
    _AUTH_POLICY_DEFAULT_NAME,
    ClickExceptionWithContext,
    _do_login,
    _do_logout_endpoints,
    _do_render_user_config,
    app,
    create_or_choose_auth_project,
    init_config_dir,
)
from globus_compute_endpoint.endpoint.config import (
    ManagerEndpointConfig,
    UserEndpointConfig,
)
from globus_compute_endpoint.endpoint.config.utils import load_config_yaml
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_endpoint.endpoint.identity_mapper import MappedPosixIdentity
from globus_compute_sdk.sdk.auth.auth_client import ComputeAuthClient
from globus_compute_sdk.sdk.auth.globus_app import UserApp
from globus_compute_sdk.sdk.compute_dir import ensure_compute_dir
from globus_sdk import MISSING
from pytest_mock import MockFixture

_MOCK_BASE = "globus_compute_endpoint.cli."


@pytest.fixture(autouse=True, scope="module")
def reset_umask():
    original_umask = os.umask(0)
    os.umask(original_umask)
    yield
    os.umask(original_umask)


@pytest.fixture
def gc_dir(tmp_path):
    yield tmp_path / ".globus_compute"


@pytest.fixture
def ep_name(randomstring):
    yield randomstring()


@pytest.fixture
def mock_app():
    mock_app = mock.Mock(spec=UserApp)
    with mock.patch(f"{_MOCK_BASE}get_globus_app_with_scopes", return_value=mock_app):
        yield mock_app


@pytest.fixture
def mock_auth_client(mock_app):
    mock_auth_client = mock.Mock(spec=ComputeAuthClient)
    mock_auth_client._app = mock_app
    with mock.patch(f"{_MOCK_BASE}ComputeAuthClient", return_value=mock_auth_client):
        yield mock_auth_client


@pytest.fixture
def mock_command_ensure(gc_dir):
    with mock.patch(f"{_MOCK_BASE}CommandState.ensure") as m:
        m.return_value = m
        m.endpoint_config_dir = gc_dir

        yield m


@pytest.fixture
def mock_ep(gc_dir, ep_name):
    with mock.patch(f"{_MOCK_BASE}Endpoint") as m:
        ep_dir = gc_dir / ep_name
        m.get_endpoint_by_name_or_uuid.return_value = ep_dir
        m.pid_path.return_value = ep_dir / "daemon.pid"
        m.return_value = m
        yield m


@pytest.fixture
def mock_load_config_yaml():
    conf = UserEndpointConfig()
    with mock.patch(f"{_MOCK_BASE}load_config_yaml") as m:
        m.return_value = conf
        yield m


@pytest.fixture
def mock_get_config():
    conf = UserEndpointConfig()
    with mock.patch(f"{_MOCK_BASE}get_config") as m:
        m.return_value = conf
        yield m


@pytest.fixture
def mock_render_config_user_template():
    with mock.patch(f"{_MOCK_BASE}render_config_user_template") as m:
        yield m


@pytest.fixture
def make_endpoint_dir(mock_command_ensure, ep_name):
    def func(name=ep_name, ep_uuid=None):
        ep_dir = mock_command_ensure.endpoint_config_dir / name
        ep_dir.mkdir(parents=True, exist_ok=True)
        if ep_uuid is not None:
            ep_json = ep_dir / "endpoint.json"
            ep_json.write_text(json.dumps({"endpoint_id": ep_uuid}))
        ep_config = Endpoint._config_file_path(ep_dir)
        ep_config.write_text("""
display_name: null
engine:
    type: ThreadPoolEngine
            """.strip())
        return ep_dir

    return func


@pytest.fixture
def make_manager_endpoint_dir(mock_command_ensure, ep_name):
    def func(name=ep_name, ep_uuid=None):
        ep_dir = mock_command_ensure.endpoint_config_dir / name
        ep_dir.mkdir(parents=True, exist_ok=True)
        if ep_uuid is not None:
            ep_json = ep_dir / "endpoint.json"
            ep_json.write_text(json.dumps({"endpoint_id": ep_uuid}))
        ep_config = Endpoint._config_file_path(ep_dir)
        ep_template = Endpoint.user_config_template_path(ep_dir)
        ep_schema = Endpoint.user_config_schema_path(ep_dir)
        ep_config.write_text("""
display_name: null
            """.strip())
        ep_template.write_text("""
heartbeat_period: {{ heartbeat }}
engine:
    type: ThreadPoolEngine
            """.strip())
        ep_schema.write_text("""
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "heartbeat": { "type": "number" }
  }
}
            """.strip())
        return ep_dir

    return func


@pytest.fixture
def cli_runner():
    return CliRunner(mix_stderr=False)


@pytest.fixture
def run_line(cli_runner):
    def func(argline, *, assert_exit_code: int | None = 0, stdin=None):
        args = shlex.split(argline) if isinstance(argline, str) else argline

        if stdin is None:
            stdin = "{}"  # silence some logs; incurred by invoke's sys.stdin choice
        result = cli_runner.invoke(app, args, input=stdin)
        if assert_exit_code is not None:
            assert result.exit_code == assert_exit_code, (
                result.stdout,
                result.stderr,
                result.exception,
            )
        return result

    return func


@pytest.mark.parametrize(
    "cause", [None, Exception("boom"), ClickException("click boom")]
)
def test_click_exception_with_context(cause, randomstring):
    message = randomstring()
    with pytest.raises(ClickExceptionWithContext) as pyt_exc:
        raise ClickExceptionWithContext(message) from cause

    if cause is not None:
        exc = pyt_exc.value
        assert exc.__cause__ is cause
        assert message in str(exc)
        assert type(cause).__name__ in str(exc)
        assert str(cause) in str(exc)


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


def test_start_endpoint_no_such_ep(run_line, mock_ep, ep_name):
    res = run_line(f"start {ep_name}", assert_exit_code=1)
    mock_ep.start_endpoint.assert_not_called()
    assert "no endpoint configuration on this machine at " in res.stderr
    assert ep_name in res.stderr


def test_start_endpoint_existing_ep(run_line, mock_ep, make_endpoint_dir, ep_name):
    make_endpoint_dir()
    run_line(f"start {ep_name}")
    mock_ep.start_endpoint.assert_called_once()


def test_start_endpoint_already_running(make_endpoint_dir, ep_name):
    """Check to ensure endpoint already active message prints to console"""
    ep_dir = make_endpoint_dir()
    pid_path = Endpoint.pid_path(ep_dir)
    pid_path.write_text("12345")
    f = io.StringIO()
    with redirect_stderr(f):
        with pytest.raises(SystemExit) as pyt_e:
            cli._do_start_endpoint(ep_dir=ep_dir, endpoint_uuid=None)
    pid_path.unlink()

    serr = f.getvalue()
    assert pyt_e.value.code == os.EX_CANTCREAT
    assert "Another instance " in serr, "Expect cause explained"
    assert "Refusing to start." in serr, "Expect action taken conveyed"
    assert "remove the PID file" in serr, "Expect suggested action conveyed"


def test_start_endpoint_stale(mock_ep, make_endpoint_dir, ep_name):
    ep_dir = make_endpoint_dir()
    pid_path = Endpoint.pid_path(ep_dir)
    pid_path.write_text("12345")
    stale_time = time.time() - 95  # something larger than HB * 3
    os.utime(pid_path, (stale_time, stale_time))
    f = io.StringIO()
    with redirect_stderr(f):
        cli._do_start_endpoint(ep_dir=ep_dir, endpoint_uuid=None)

    serr = f.getvalue()
    assert "Previous endpoint instance" in serr, "Expect 'who' in warning"
    assert "failed to shutdown cleanly" in serr, "Expect 'what' in warning"
    assert "Removing PID file" in serr, "Expect action taken in warning"


@pytest.mark.parametrize("is_uep", (False, True))
def test_start_non_template_emits_upgrade_message(mock_ep, make_endpoint_dir, is_uep):
    ep_dir = make_endpoint_dir()
    f = io.StringIO()
    with redirect_stdout(f):
        cli._do_start_endpoint(
            ep_dir=ep_dir, endpoint_uuid=None, die_with_parent=is_uep
        )

    assert ("migrate-to-template-capable" in f.getvalue()) is not is_uep


@pytest.mark.parametrize("cli_cmd", ["configure"])
def test_endpoint_uuid_name_not_supported(run_line, cli_cmd):
    ep_uuid_name = uuid.uuid4()
    res = run_line(f"{cli_cmd} {ep_uuid_name}", assert_exit_code=2)
    assert (
        cli_cmd in res.stderr
        and "requires an endpoint name that is not a UUID" in res.stderr
    )


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
        (True, json.dumps({"amqp_creds": {}, "config": "myconfig", "audit_fd": 1})),
    ],
)
def test_start_ep_reads_stdin(
    mocker, run_line, mock_ep, mock_get_config, make_endpoint_dir, stdin_data, ep_name
):
    data_is_valid, data = stdin_data

    mock_load_conf = mocker.patch(f"{_MOCK_BASE}load_config_yaml")
    mock_load_conf.return_value = mock_get_config.return_value

    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mock_sys = mocker.patch(f"{_MOCK_BASE}sys")
    mock_sys.stdin.closed = False
    mock_sys.stdin.isatty.return_value = False
    mock_sys.stdin.read.return_value = data

    make_endpoint_dir()

    run_line(f"start {ep_name}")
    assert mock_ep.start_endpoint.called
    s_ep_a, _ = mock_ep.start_endpoint.call_args
    reg_info_found = s_ep_a[5]
    audit_fd_found = s_ep_a[8]

    if data_is_valid:
        data_dict = json.loads(data)
        reg_info = data_dict.get("amqp_creds", {})
        config_str = data_dict.get("config")
        audit_fd = data_dict.get("audit_fd")

        assert reg_info_found == reg_info
        assert audit_fd_found == audit_fd
        if config_str:
            config_str_found = mock_load_conf.call_args[0][0]
            assert config_str_found == config_str

    else:
        assert mock_get_config.called
        assert mock_log.debug.called
        a, k = mock_log.debug.call_args
        assert "Invalid info on stdin" in a[0]
        assert reg_info_found == {}


@pytest.mark.parametrize("fn_count", range(-1, 5))
def test_start_ep_stdin_allowed_fns_overrides_conf(
    mocker, run_line, mock_ep, mock_get_config, make_endpoint_dir, ep_name, fn_count
):
    if fn_count == -1:
        allowed_fns = None
    else:
        allowed_fns = tuple(str(uuid.uuid4()) for _ in range(fn_count))

    # to be overridden
    mock_get_config.return_value.allowed_functions = [uuid.uuid4() for _ in range(5)]

    mock_sys = mocker.patch(f"{_MOCK_BASE}sys")
    mock_sys.stdin.closed = False
    mock_sys.stdin.isatty.return_value = False
    mock_sys.stdin.read.return_value = json.dumps({"allowed_functions": allowed_fns})

    make_endpoint_dir()

    run_line(f"start {ep_name}")
    assert mock_ep.start_endpoint.called
    (_, _, found_conf, *_), _k = mock_ep.start_endpoint.call_args
    assert found_conf.allowed_functions == allowed_fns, "allowed field not overridden!"


@pytest.mark.parametrize("use_uuid", (True, False))
def test_stop_endpoint(
    run_line, mock_ep, mock_get_config, make_endpoint_dir, ep_name, use_uuid
):
    ep_uuid = str(uuid.uuid4()) if use_uuid else None
    make_endpoint_dir(ep_uuid=ep_uuid)
    run_line(f"stop {ep_uuid if use_uuid else ep_name}")
    mock_ep.stop_endpoint.assert_called_once()


def test_restart_endpoint_does_start_and_stop(
    run_line, mock_ep, make_endpoint_dir, ep_name
):
    make_endpoint_dir()
    run_line(f"restart {ep_name}")

    mock_ep.stop_endpoint.assert_called_once()
    mock_ep.start_endpoint.assert_called_once()


@mock.patch(f"{_MOCK_BASE}setup_logging")
def test_debug_configurable(
    mock_setup_log, run_line, mock_ep, mock_command_ensure, ep_name
):
    mock_command_ensure.debug = False
    ep_dir = mock_command_ensure.endpoint_config_dir / ep_name
    ep_dir.mkdir(parents=True)
    config = {"debug": False, "engine": {"type": "ThreadPoolEngine"}}
    data = {"config": yaml.safe_dump(config)}

    run_line(f"start {ep_name}", stdin=json.dumps(data), assert_exit_code=None)

    _a, k = mock_setup_log.call_args
    assert mock_command_ensure.debug is False, "Verify test setup"
    assert "debug" in k
    assert k["debug"] is False, "Null test: stays false"

    mock_setup_log.reset_mock()
    config["debug"] = True
    data["config"] = yaml.safe_dump(config)

    run_line(f"start {ep_name}", stdin=json.dumps(data), assert_exit_code=None)

    _a, k = mock_setup_log.call_args
    assert mock_command_ensure.debug is False, "Verify test setup"
    assert "debug" in k
    assert k["debug"] is True, "Expect config sets debug"


@mock.patch(f"{_MOCK_BASE}setup_logging")
def test_cli_debug_overrides_config(
    mock_setup_log, run_line, mock_ep, mock_command_ensure, ep_name
):
    mock_command_ensure.debug = True
    ep_dir = mock_command_ensure.endpoint_config_dir / ep_name
    ep_dir.mkdir(parents=True)
    config = {"debug": False, "engine": {"type": "ThreadPoolEngine"}}
    data = {"config": yaml.safe_dump(config)}

    run_line(f"start {ep_name}", stdin=json.dumps(data), assert_exit_code=None)

    _a, k = mock_setup_log.call_args
    assert mock_command_ensure.debug is True, "Verify test setup"
    assert "debug" in k
    assert k["debug"] is True, "Expect --debug flag overrides config"


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
    dir_name, display_name = display_test

    conf = mock_command_ensure.endpoint_config_dir / dir_name / "config.yaml"
    configure_arg = ""
    if display_name is not None:
        configure_arg = f" --display-name '{display_name}'"
    run_line(f"configure {dir_name}{configure_arg}")

    with open(conf) as f:
        conf_dict = yaml.safe_load(f)

    assert conf_dict["display_name"] == display_name


@pytest.mark.parametrize(
    ("ep_name", "set_ha"),
    (
        ["ep0", None],
        ["ep1", False],
        ["ep2", True],
    ),
)
def test_start_ep_high_assurance_in_config(
    run_line, mock_command_ensure, make_endpoint_dir, ep_name, set_ha
):
    conf = mock_command_ensure.endpoint_config_dir / ep_name / "config.yaml"
    configure_arg = ""
    auth_policy = str(uuid.uuid4())
    sub_id = str(uuid.uuid4())
    if set_ha is not None:
        configure_arg = (
            f" --subscription-id {sub_id} --auth-policy "
            f"{auth_policy} {'--high-assurance' if set_ha else ''}"
        )
    run_line(f"configure {ep_name}{configure_arg}")

    with open(conf) as f:
        conf_dict = yaml.safe_load(f)

    if set_ha is True:
        assert conf_dict["high_assurance"] == set_ha
    else:
        assert conf_dict.get("high_assurance", False) is False


def test_configure_ep_auth_policy_in_config(
    run_line, mock_command_ensure, make_endpoint_dir
):
    ep_name = "my-ep"
    auth_policy = str(uuid.uuid4())
    conf = mock_command_ensure.endpoint_config_dir / ep_name / "config.yaml"

    run_line(f"configure {ep_name} --auth-policy {auth_policy}")

    with open(conf) as f:
        conf_dict = yaml.safe_load(f)

    assert conf_dict["authentication_policy"] == auth_policy


def test_configure_ep_subscription_id_in_config(
    run_line, mock_command_ensure, make_endpoint_dir
):
    ep_name = "my-ep"
    subscription_id = str(uuid.uuid4())
    conf = mock_command_ensure.endpoint_config_dir / ep_name / "config.yaml"

    run_line(f"configure {ep_name} --subscription-id {subscription_id}")

    with open(conf) as f:
        conf_dict = yaml.safe_load(f)

    assert conf_dict["subscription_id"] == subscription_id


def test_configure_ep_endpoint_config_deprecated(run_line, randomstring, mock_ep):
    ep_config_arg = randomstring()

    with pytest.warns(DeprecationWarning, match="--endpoint-config is deprecated"):
        run_line(
            f"configure {randomstring()} --endpoint-config {ep_config_arg}",
            assert_exit_code=0,
        )

    assert (
        mock_ep.configure_endpoint.call_args.kwargs["endpoint_config"].name
        == ep_config_arg
    )


def test_configure_ep_manager_config_precedence(
    run_line, randomstring, mock_ep, gc_dir
):
    ep_config_arg = randomstring(5)
    gc_dir.mkdir(parents=True, exist_ok=True)
    manager_config_arg = gc_dir / randomstring(4)
    manager_config_arg.touch()

    with pytest.warns(UserWarning, match="--endpoint-config will be ignored"):
        run_line(
            f"configure {randomstring()} --manager-config {manager_config_arg}"
            f" --endpoint-config {ep_config_arg}",
            assert_exit_code=0,
        )

    assert (
        mock_ep.configure_endpoint.call_args.kwargs["endpoint_config"]
        == manager_config_arg
    )


@pytest.mark.parametrize("manager_config", ("some-config.yaml", None))
@pytest.mark.parametrize("template_config", ("some-template.yaml.j2", None))
def test_configure_ep_config_options(
    run_line, randomstring, gc_dir, mock_ep, manager_config, template_config
):
    gc_dir.mkdir(parents=True, exist_ok=True)

    cmd = f"configure {randomstring()}"
    if manager_config:
        manager_config = gc_dir / manager_config
        manager_config.touch()
        cmd += f" --manager-config {manager_config}"
    if template_config:
        template_config = gc_dir / template_config
        template_config.touch()
        cmd += f" --template-config {template_config}"

    run_line(cmd, assert_exit_code=0)

    call_kwargs = mock_ep.configure_endpoint.call_args.kwargs
    assert call_kwargs["endpoint_config"] == manager_config
    assert call_kwargs["user_config_template"] == template_config


@pytest.mark.parametrize("display_name", [None, "None"])
def test_config_yaml_display_none(run_line, mock_command_ensure, display_name):
    ep_name = "test_display_none"

    conf = mock_command_ensure.endpoint_config_dir / ep_name / "config.yaml"

    config_cmd = f"configure {ep_name}"
    if display_name is not None:
        config_cmd += f" --display-name {display_name}"
    run_line(config_cmd)

    conf_dict = dict(yaml.safe_load(conf.read_text()))
    conf = load_config_yaml(yaml.safe_dump(conf_dict))

    assert conf.display_name is None, conf.display_name


def test_start_ep_incorrect_config_yaml(
    run_line, mock_command_ensure, make_endpoint_dir, ep_name
):
    conf = make_endpoint_dir() / "config.yaml"

    conf.write_text("asdf")
    res = run_line(f"start {ep_name}", assert_exit_code=1)
    assert "Invalid config syntax" in res.stderr


def test_start_ep_incorrect_config_py(
    run_line, mock_command_ensure, make_endpoint_dir, ep_name
):
    conf = make_endpoint_dir() / "config.py"

    conf.write_text("asa asd df = 5")  # fail the import
    with mock.patch(f"{_MOCK_BASE}log"):
        with mock.patch(
            "globus_compute_endpoint.endpoint.config.utils.log"
        ) as mock_util_log:
            res = run_line(f"start {ep_name}", assert_exit_code=1)
    a, _ = mock_util_log.exception.call_args
    assert "might be out of date" in a[0]
    assert isinstance(res.exception, SyntaxError)

    # `coverage` demands a valid syntax file.  FBOW, then, the ordering and
    # commingling of these two tests is intentional.  Bit of a meta problem ...
    conf.unlink()
    conf.write_text("asdf = 5")  # syntactically correct
    res = run_line(f"start {ep_name}", assert_exit_code=1)
    assert "modified incorrectly?" in res.stderr


@mock.patch("globus_compute_endpoint.endpoint.config.utils.load_config_yaml")
def test_start_ep_config_py_takes_precedence(
    mock_load, run_line, mock_ep, make_endpoint_dir, ep_name
):
    conf_py = make_endpoint_dir() / "config.py"
    conf_py.write_text(
        "from globus_compute_endpoint.endpoint.config import UserEndpointConfig"
        "\nconfig = UserEndpointConfig()"
    )

    run_line(f"start {ep_name}")
    assert mock_ep.start_endpoint.called
    assert not mock_load.called, "Key outcome: config.py takes precedence"


def test_start_ep_umask_set_restrictive(run_line, make_endpoint_dir, ep_name, mock_ep):
    orig_umask = os.umask(0)
    make_endpoint_dir()
    run_line(f"start {ep_name}")
    assert os.umask(orig_umask) == 0o077


@pytest.mark.parametrize("use_uuid", (True, False))
@mock.patch(f"{_MOCK_BASE}Endpoint.get_endpoint_id")
def test_delete_endpoint(
    get_endpoint_id,
    mock_get_config,
    run_line,
    mock_ep,
    ep_name,
    ep_uuid,
    make_endpoint_dir,
    use_uuid,
):
    get_endpoint_id.return_value = ep_uuid

    make_endpoint_dir(ep_uuid=ep_uuid)
    run_line(f"delete {ep_uuid if use_uuid else ep_name} --yes")
    mock_ep.delete_endpoint.assert_called_once()
    assert mock_ep.delete_endpoint.call_args[1]["ep_uuid"] == ep_uuid
    if use_uuid:
        get_endpoint_id.assert_not_called()
    else:
        get_endpoint_id.assert_called()


@mock.patch("globus_compute_endpoint.endpoint.endpoint.Endpoint.get_funcx_client")
def test_delete_endpoint_with_malformed_config_sc28515(
    _mock_func, fs, run_line, ep_name
):
    conf_dir = ensure_compute_dir() / ep_name
    conf_dir.mkdir()
    config = {"engine": {"type": "ThreadPoolEngine"}}
    (conf_dir / "config.yaml").write_text(yaml.safe_dump(config))
    assert conf_dir.exists() and conf_dir.is_dir()
    run_line(f"delete {ep_name} --yes --force")
    assert not conf_dir.exists()


@pytest.mark.parametrize("die_with_parent", [True, False])
def test_die_with_parent_detached(
    mock_get_config,
    run_line,
    mock_ep,
    die_with_parent,
    ep_name,
    make_endpoint_dir,
):
    make_endpoint_dir()

    if die_with_parent:
        run_line(f"start {ep_name} --die-with-parent")
    else:
        run_line(f"start {ep_name}")
    assert mock_get_config.return_value.detach_endpoint is (not die_with_parent)


def test_python_exec(mocker: MockFixture, run_line: t.Callable):
    mock_execvpe = mocker.patch("os.execvpe")
    run_line("python-exec path.to.module arg --option val")
    mock_execvpe.assert_called_with(
        sys.executable,
        [sys.executable, "-m", "path.to.module", "arg", "--option", "val"],
        os.environ,
    )


_test_name_or_uuid_decorator__data = [
    ("foo", str(uuid.uuid4())),
    ("123", str(uuid.uuid4())),
    ("nice_normal_name", str(uuid.uuid4())),
]


@pytest.mark.parametrize("name,uuid", _test_name_or_uuid_decorator__data)
def test_name_or_uuid_decorator(tmp_path, mocker, run_line, name, uuid):
    gc_conf_dir = tmp_path / ".globus_compute"
    gc_conf_dir.mkdir()
    for n, u in _test_name_or_uuid_decorator__data:
        ep_conf_dir = gc_conf_dir / n
        ep_conf_dir.mkdir()
        ep_json = ep_conf_dir / "endpoint.json"
        ep_json.write_text(json.dumps({"endpoint_id": u}))
        # dummy config.yaml so that Endpoint._get_ep_dirs finds this
        (ep_conf_dir / "config.yaml").write_text("")

    mock__do_start_endpoint = mocker.patch(f"{_MOCK_BASE}_do_start_endpoint")

    run_line(f"-c {gc_conf_dir} start {name}")
    run_line(f"-c {gc_conf_dir} start {uuid}")

    assert mock__do_start_endpoint.call_count == 2

    first_result, second_result = (
        call.kwargs["ep_dir"] for call in mock__do_start_endpoint.call_args_list
    )

    assert first_result == second_result

    assert first_result is not None
    assert second_result is not None


@pytest.mark.parametrize(
    "data",
    [
        ("foo", "no endpoint configuration on this machine"),
        (str(uuid.uuid4()), "no endpoint configuration on this machine with ID"),
    ],
)
def test_get_endpoint_by_name_or_uuid_error_message(tmp_path, run_line, data):
    value, error = data

    with mock.patch(f"{_MOCK_BASE}get_config_dir", return_value=tmp_path):
        result = run_line(f"start {value}", assert_exit_code=1)

    assert error in result.stderr


@pytest.mark.parametrize(
    "cmd,ep_method,auth_err_msg",
    [
        ("start", "start_endpoint", '{"error":"invalid_grant"}'),
        ("start", "start_endpoint", '{"error":"something else"}'),
        ("start", "start_endpoint", ""),
        ("stop", "stop_endpoint", "err_msg"),
        ("delete --yes", "delete_endpoint", "err_msg"),
    ],
)
def test_handle_globus_auth_error(
    mocker: MockFixture,
    run_line,
    mock_ep,
    make_endpoint_dir,
    ep_name,
    cmd,
    ep_method,
    auth_err_msg,
):
    make_endpoint_dir()

    mock_log = mocker.patch("globus_compute_endpoint.exception_handling.log")
    mock_resp = mock.MagicMock(
        status_code=400,
        reason="Bad Request",
        text=auth_err_msg,
    )
    mocker.patch.object(
        mock_ep,
        ep_method,
        side_effect=globus_sdk.AuthAPIError(r=mock_resp),
    )

    res = run_line(f"{cmd} {ep_name}", assert_exit_code=os.EX_NOPERM)

    err_msg = "An Auth API error occurred."
    a, k = mock_log.warning.call_args

    assert err_msg in res.stdout
    assert err_msg in a[0]
    assert "400" in res.stdout
    assert "400" in a[0]

    additional_details = "credentials may have expired"
    if "invalid_grant" in auth_err_msg:
        assert additional_details in res.stdout
    else:
        assert additional_details not in res.stdout


@pytest.mark.parametrize("exit_exc", (None, SystemExit(), SystemExit(0)))
def test_happy_path_exit_no_amqp_msg(
    mocker,
    run_line,
    mock_ep,
    make_endpoint_dir,
    ep_name,
    exit_exc,
):
    mock_send = mocker.patch(f"{_MOCK_BASE}send_endpoint_startup_failure_to_amqp")
    make_endpoint_dir()

    stdin = json.dumps({"amqp_creds": {"some": "data"}})
    if exit_exc is not None:
        mock_ep.start_endpoint.side_effect = exit_exc
    run_line(f"start {ep_name}", assert_exit_code=0, stdin=stdin)
    assert mock_ep.start_endpoint.called
    assert not mock_send.called


@pytest.mark.parametrize(
    "ec,exit_exc",
    (
        (1, SystemExit("Death!")),
        (5, SystemExit(5)),
        (1, RuntimeError("fool!")),
        (1, MemoryError("Oh no!")),
        (1, AssertionError("mistake")),
        (1, Exception("Generally no good.")),
    ),
)
def test_fail_exit_sends_amqp_msg(
    mocker,
    run_line,
    mock_ep,
    make_endpoint_dir,
    ep_name,
    ec,
    exit_exc,
):
    mock_send = mocker.patch(f"{_MOCK_BASE}send_endpoint_startup_failure_to_amqp")
    make_endpoint_dir()

    stdin = json.dumps({"amqp_creds": {"some": "data"}})
    mock_ep.start_endpoint.side_effect = exit_exc
    run_line(f"start {ep_name}", assert_exit_code=ec, stdin=stdin)
    assert mock_ep.start_endpoint.called
    assert mock_send.called


@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("login_required", [True, False])
def test_login(
    force: bool,
    login_required: bool,
    caplog: pytest.LogCaptureFixture,
    mock_app: UserApp,
):
    mock_app.login_required.return_value = login_required
    caplog.set_level(logging.INFO)

    _do_login(force=force)

    if login_required or force:
        assert mock_app.login.call_count == 1
    else:
        assert mock_app.login.call_count == 0
        assert "Already logged in" in caplog.text


@pytest.mark.parametrize("force", [True, False], ids=["forced", "unforced"])
def test_login_handles_partial_client_login_state(monkeypatch, force):
    monkeypatch.setenv("GLOBUS_COMPUTE_CLIENT_SECRET", "some_uuid")
    with pytest.raises(ClickException) as e:
        _do_login(force)
    assert "both environment variables" in str(e)


@pytest.mark.parametrize("force", [True, False])
@pytest.mark.parametrize("running_endpoints", [{}, {"my_ep": {"status": "Running"}}])
def test_logout(
    force: bool,
    running_endpoints: dict,
    caplog: pytest.LogCaptureFixture,
    mocker: MockFixture,
    mock_command_ensure,
    mock_app: UserApp,
):
    mocker.patch(
        f"{_MOCK_BASE}Endpoint.get_running_endpoints", return_value=running_endpoints
    )
    caplog.set_level(logging.INFO)

    _do_logout_endpoints(force=force)

    if running_endpoints and not force:
        assert "endpoints are currently running" in caplog.text
    elif not running_endpoints or force:
        assert mock_app.logout.call_count == 1
        assert "Logout succeeded" in caplog.text
    else:
        assert mock_app.logout.call_count == 0


@pytest.mark.parametrize("ap_project_id", [None, "foo"])
@pytest.mark.parametrize("ap_display_name", [None, "foo"])
@pytest.mark.parametrize("ap_description", [None, "foo"])
@pytest.mark.parametrize("ap_allowed", [None, "foo"])
@pytest.mark.parametrize("ap_exclude", [None, "foo"])
@pytest.mark.parametrize("ap_timeout", [None, "1"])
def test_configure_ep_auth_policy_mutually_exclusive(
    run_line,
    mock_command_ensure,
    ep_name,
    ap_project_id,
    ap_display_name,
    ap_description,
    ap_allowed,
    ap_exclude,
    ap_timeout,
):
    params = "--auth-policy=foo"
    expected_exit_code = 0
    if ap_project_id:
        params += f" --auth-policy-project-id={ap_project_id}"
        expected_exit_code = 1
    if ap_display_name:
        params += f" --auth-policy-display-name={ap_display_name}"
        expected_exit_code = 1
    if ap_description:
        params += f" --auth-policy-description={ap_description}"
        expected_exit_code = 1
    if ap_allowed:
        params += f" --allowed-domains={ap_allowed}"
        expected_exit_code = 1
    if ap_exclude:
        params += f" --excluded-domains={ap_exclude}"
        expected_exit_code = 1
    if ap_timeout:
        params += f" --auth-timeout={ap_timeout}"
        expected_exit_code = 1

    res = run_line(f"configure {params} {ep_name}", assert_exit_code=expected_exit_code)

    if expected_exit_code == 1:
        assert "at the same time" in res.stderr


def test_configure_ep_auth_policy_defaults(
    mocker,
    run_line,
    mock_ep,
    make_endpoint_dir,
    ep_name,
    mock_app: UserApp,
    mock_auth_client: ComputeAuthClient,
):
    mock_create_auth_policy = mocker.patch(f"{_MOCK_BASE}create_auth_policy")

    run_line(f"configure --auth-policy-project-id=foo {ep_name}")

    assert mock_create_auth_policy.call_args.kwargs == {
        "ac": mock_auth_client,
        "project_id": "foo",
        "display_name": _AUTH_POLICY_DEFAULT_NAME,
        "description": _AUTH_POLICY_DEFAULT_DESC,
        "include_domains": MISSING,
        "exclude_domains": MISSING,
        "high_assurance": MISSING,
        "timeout": MISSING,
        "require_mfa": MISSING,
    }


def test_configure_ep_auth_param_parse(
    mocker,
    run_line,
    mock_ep,
    ep_name,
    mock_auth_client: ComputeAuthClient,
):
    mock_create_auth_policy = mocker.patch(f"{_MOCK_BASE}create_auth_policy")
    params = " ".join(
        [
            "--high-assurance",
            "--subscription-id=sub123",
            "--auth-policy-mfa-required",
            "--auth-policy-project-id=p123",
            "--auth-policy-display-name='my awesome policy'",
            "--auth-policy-description='policy desc'",
            "--allowed-domains=xyz.com,example.org",
            "--excluded-domains=nope.com",
            "--auth-timeout=30",
        ]
    )

    run_line(f"configure {params} {ep_name}")

    assert mock_create_auth_policy.call_args.kwargs == {
        "ac": mock_auth_client,
        "project_id": "p123",
        "display_name": "my awesome policy",
        "description": "policy desc",
        "include_domains": ["xyz.com", "example.org"],
        "exclude_domains": ["nope.com"],
        "high_assurance": True,
        "timeout": 30,
        "require_mfa": True,
    }


def test_choose_auth_project(
    mocker,
    randomstring,
    mock_auth_client: ComputeAuthClient,
):
    mock_user_input_select = mocker.patch(f"{_MOCK_BASE}user_input_select")
    mock_user_input_select.side_effect = lambda _p, o: random.choice(o)

    get_projects_response = [
        {"id": str(uuid.uuid4()), "display_name": randomstring()} for _ in range(5)
    ]
    mock_auth_client.get_projects.return_value = get_projects_response

    proj_id = create_or_choose_auth_project(mock_auth_client)

    assert uuid.UUID(proj_id)
    assert proj_id in [p["id"] for p in get_projects_response]


@pytest.mark.parametrize("has_projects", [True, False])
def test_configure_ep_auth_policy_creates_or_chooses_project(
    mocker,
    run_line,
    ep_name,
    mock_auth_client: ComputeAuthClient,
    randomstring,
    has_projects,
):
    class StopTest(Exception):
        pass

    if has_projects:
        mock_auth_client.get_projects.return_value = [
            {"id": str(uuid.uuid4()), "display_name": randomstring()} for _ in range(5)
        ]
        # stop test when choosing a project
        mocker.patch(f"{_MOCK_BASE}user_input_select", side_effect=StopTest)
    else:
        mock_auth_client.get_projects.return_value = []
        mock_input = mocker.patch("builtins.input")
        mock_input.return_value = "y"  # break out of interact loop asap
        # stop test when creating a project
        mock_auth_client.create_project.side_effect = StopTest

    res = run_line(
        f"configure --auth-policy-display-name=foo {ep_name}", assert_exit_code=1
    )

    assert isinstance(res.exception, StopTest)


@pytest.mark.parametrize(
    (
        "is_ha",
        "use_mfa",
        "policy_id",
        "auth_desc",
        "allowed_domains",
        "sub_id",
        "exc_text",
    ),
    (
        (
            [True, False, "pid", None, None, "sub_id", None],
            [True, True, None, "desc", "globus.org", "sub_id", None],
            [
                False,
                True,
                None,
                "desc",
                "globus.org",
                "sub_id",
                "MFA may only be enabled for High Assurance",
            ],
            [
                True,
                True,
                "pid",
                None,
                None,
                "sub_id",
                "MFA may only be specified when creating a policy",
            ],
            [
                True,
                False,
                "pid",
                "desc",
                "globus.org",
                "sub_id",
                "Cannot specify an existing",
            ],
            [True, False, "pid", "desc", None, "sub_id", "Cannot specify an existing"],
            [
                True,
                False,
                None,
                None,
                None,
                "sub_id",
                "require both a HA policy and a HA sub",
            ],
            [
                True,
                False,
                "pid",
                None,
                None,
                None,
                "require both a HA policy and a HA sub",
            ],
            [
                True,
                False,
                None,
                "auth_desc",
                "globus.org",
                None,
                "require both a HA policy and a HA sub",
            ],
            [False, False, None, "auth_desc", "globus.org", None, None],
            [False, False, "pid", None, None, None, None],
        )
    ),
)
def test_configure_ha_ep_requirements(
    mocker,
    run_line,
    mock_ep,
    mock_auth_client,
    is_ha: bool,
    use_mfa: bool,
    policy_id: str | None,
    auth_desc: str | None,
    allowed_domains: str | None,
    sub_id: str | None,
    exc_text: str | None,
):
    mock_auth_client.create_policy.return_value = {"policy": {"id": "foo"}}
    mock_auth_client.get_projects.return_value = []
    mocker.patch(f"{_MOCK_BASE}create_or_choose_auth_project")

    args = ["configure"]
    if is_ha:
        args.append("--high-assurance")
    if policy_id:
        args.append(f"--auth-policy {policy_id}")
    if auth_desc:
        args.append(f"--auth-policy-description {auth_desc}")
    if allowed_domains:
        args.append(f"--allowed-domains {allowed_domains}")
    if sub_id:
        args.append(f"--subscription-id {sub_id}")
    if use_mfa:
        args.append("--auth-policy-mfa-required")

    args.append("ep_name")

    line = " ".join(args)
    if exc_text:
        res = run_line(line, assert_exit_code=1)
        assert exc_text in res.stderr
    else:
        run_line(line)
        assert mock_ep.configure_endpoint.called


@pytest.mark.parametrize(
    ("delete_cmd", "use_uuid", "exit_code", "delete_done"),
    [
        ("delete --yes {ep_info}", True, 0, True),
        ("delete --force {ep_info}", False, None, False),
        ("delete --yes --force {ep_info}", False, 0, True),
    ],
)
def test_delete_endpoint_local(
    run_line,
    mock_ep,
    mock_get_config,
    make_endpoint_dir,
    ep_name,
    delete_cmd,
    use_uuid,
    exit_code,
    delete_done,
):
    ep_info = str(uuid.uuid4()) if use_uuid else ep_name
    make_endpoint_dir(ep_uuid=ep_info if use_uuid else None)
    run_line(delete_cmd.format(ep_info=ep_info), assert_exit_code=exit_code)
    assert delete_done == bool(mock_ep.delete_endpoint.called)


def test_delete_endpoint_local_uuid(mocker, run_line, mock_ep):
    mock_ep.get_endpoint_dir_by_uuid.return_value = None
    mocker.patch("click.confirm").return_value = True
    run_line(f"delete {uuid.uuid4()}", assert_exit_code=1)
    assert not mock_ep.delete_endpoint.called


@pytest.mark.parametrize(
    ("delete_args", "err_msg"),
    [
        (
            "--yes fake_uuid",
            "no endpoint configuration",
        ),
    ],
)
def test_delete_endpoint_no_local_config(
    run_line, mock_ep, make_endpoint_dir, delete_args, err_msg
):
    line = f"delete --yes {delete_args}"
    result = run_line(line, assert_exit_code=1)
    assert not mock_ep.delete_endpoint.called
    assert err_msg in result.stderr


def test_render_user_config_happy_path(
    run_line,
    mock_render_config_user_template,
    make_manager_endpoint_dir,
    ep_name,
    randomstring,
):
    mock_render_result = randomstring()
    mock_render_config_user_template.return_value = mock_render_result
    ep_dir = make_manager_endpoint_dir()

    user_options = {"heartbeat": "some_value"}
    user_options_path = ep_dir / "user_options.json"
    user_options_path.write_text(json.dumps(user_options))

    result = run_line(
        f"render-user-config -e {ep_name} --user-options {user_options_path}",
        assert_exit_code=0,
    )

    assert mock_render_result in result.stdout
    mock_render_config_user_template.assert_called_once()
    for _, v in mock_render_config_user_template.call_args.kwargs.items():
        assert v is not None


@pytest.mark.parametrize(
    "option,contents",
    [
        pytest.param("--template", "template: value\n", id="template"),
        pytest.param("--user-options", '{"foo": "bar"}', id="user-options"),
        pytest.param("--user-schema", '{"type": "object"}', id="user-schema"),
        pytest.param("--parent-config", "display_name: parent\n", id="parent-config"),
        pytest.param("--user-runtime", '{"runtime": "val"}', id="user-runtime"),
        pytest.param(
            "--mapped-identity",
            '{"local_user_record": null, "matched_identity": null, "globus_identity_candidates": null}',  # noqa: E501
            id="mapped-identity",
        ),
    ],
)
def test_render_user_config_file_options(
    run_line,
    make_manager_endpoint_dir,
    mock_render_config_user_template,
    randomstring,
    option,
    contents,
):
    ep_dir = make_manager_endpoint_dir()

    file_path = ep_dir / randomstring()
    file_path.write_text(contents)

    def fake_render_config_user_template(**kwargs):
        out = ""
        for v in kwargs.values():
            if isinstance(v, dict):
                out += json.dumps(v)
            elif isinstance(v, str):
                out += v
            elif isinstance(v, ManagerEndpointConfig):
                out += yaml.safe_dump({"display_name": v.display_name})
            elif isinstance(v, MappedPosixIdentity):
                out += json.dumps(
                    {
                        "local_user_record": v.local_user_record,
                        "matched_identity": (
                            str(v.matched_identity) if v.matched_identity else None
                        ),
                        "globus_identity_candidates": v.globus_identity_candidates,
                    }
                )
        return out

    mock_render_config_user_template.side_effect = fake_render_config_user_template

    result = run_line(
        f"render-user-config -e {ep_dir.name} {option} {file_path}",
        assert_exit_code=0,
    )

    assert contents in result.stdout


@pytest.mark.parametrize(
    "parameter",
    [
        param.name
        for param in cli.render_user_config.params
        if isinstance(param.type, ClickFile)
    ],
)
def test_render_user_config_stdin(run_line, mocker, randomstring, parameter):
    mock__do_render = mocker.patch(f"{_MOCK_BASE}_do_render_user_config")
    stdin_text = randomstring()
    option = parameter.replace("_", "-")

    run_line(f"render-user-config --{option} -", stdin=stdin_text)

    mock__do_render.assert_called_once()
    call_args = mock__do_render.call_args.kwargs
    assert call_args[parameter + "_file"].read() == stdin_text


@pytest.mark.parametrize(
    "option",
    [
        param.name.replace("_", "-")
        for param in cli.render_user_config.params
        if isinstance(param.type, ClickFile)
        and param.name is not None
        and param.name not in {"template"}
    ],
)
def test_render_user_config_file_option_malformed(
    run_line,
    make_manager_endpoint_dir,
    randomstring,
    option,
):
    ep_dir = make_manager_endpoint_dir()

    file_path = ep_dir / randomstring()
    file_path.write_text(randomstring())  # malformed content

    result = run_line(
        f"render-user-config -e {ep_dir.name} --{option} {file_path}",
        assert_exit_code=1,
    )

    assert "Invalid" in result.stderr
    assert option.replace("-", " ") in result.stderr


def test_render_user_config_calls__do_render(run_line, mocker):
    mock__do_render = mocker.patch(f"{_MOCK_BASE}_do_render_user_config")
    run_line("render-user-config")
    mock__do_render.assert_called_once()


def test__do_render_user_config_stdin_at_most_once():
    file_params = [
        p
        for p in inspect.signature(_do_render_user_config).parameters
        if p.endswith("_file")
    ]
    num_stdin_files = random.randint(2, len(file_params))
    stdin_file_params = random.sample(file_params, num_stdin_files)

    kwargs = {p: sys.stdin if p in stdin_file_params else None for p in file_params}

    with pytest.raises(
        ClickException, match="At most one input may be read from stdin"
    ):
        _do_render_user_config(parent_ep_dir=None, **kwargs)


def test__do_render_user_config_needs_endpoint_or_template():
    with pytest.raises(
        ClickException, match="at least one of --endpoint or --template"
    ):
        _do_render_user_config(
            parent_ep_dir=None,
            template_file=None,
            user_options_file=None,
            user_schema_file=None,
            parent_config_file=None,
            user_runtime_file=None,
            mapped_identity_file=None,
        )


@pytest.mark.parametrize(
    "parent_ep_dir,parent_config_file",
    [
        pytest.param(pathlib.Path("/some/path"), None, id="parent_ep_dir"),
        pytest.param(None, io.StringIO(""), id="parent_config_file"),
    ],
)
def test__do_render_user_config_checks_template_capable(
    mock_load_config_yaml, mock_get_config, parent_ep_dir, parent_config_file
):
    e = "test setup: mocked config should never be template capable"
    assert not isinstance(mock_get_config.return_value, ManagerEndpointConfig), e
    assert not isinstance(mock_load_config_yaml.return_value, ManagerEndpointConfig), e

    with pytest.raises(ClickException, match="does not support templating"):
        _do_render_user_config(
            parent_ep_dir=parent_ep_dir,
            parent_config_file=parent_config_file,
            template_file=mock.Mock(),
            user_options_file=None,
            user_schema_file=None,
            user_runtime_file=None,
            mapped_identity_file=None,
        )


@pytest.mark.parametrize("template_filename", [None, "template.yaml.j2"])
@pytest.mark.parametrize("schema_filename", [None, "schema.json"])
def test__do_render_user_config_gets_paths_from_parent_ep(
    mocker, make_manager_endpoint_dir, template_filename, schema_filename
):
    ep_dir: pathlib.Path = make_manager_endpoint_dir()

    template_path = (ep_dir / template_filename) if template_filename else None
    schema_path = (ep_dir / schema_filename) if schema_filename else None

    with (ep_dir / "config.yaml").open("a") as f:
        if template_path:
            f.write(f'\nuser_config_template_path: "{template_path}"\n')
            template_path.touch()
        if schema_path:
            f.write(f'\nuser_config_schema_path: "{schema_path}"\n')
            schema_path.touch()

    mocker.patch(f"{_MOCK_BASE}render_config_user_template")
    mock_load_template = mocker.patch(f"{_MOCK_BASE}load_user_config_template")
    mock_load_schema = mocker.patch(f"{_MOCK_BASE}load_user_config_schema")

    user_options_path = ep_dir / "user_options.json"
    user_options_path.write_text(json.dumps({}))

    _do_render_user_config(
        parent_ep_dir=ep_dir,
        template_file=None,
        user_options_file=user_options_path.open("r"),
        user_schema_file=None,
        parent_config_file=None,
        user_runtime_file=None,
        mapped_identity_file=None,
    )

    mock_load_template.assert_called_once_with(
        template_path or Endpoint.user_config_template_path(ep_dir)
    )
    mock_load_schema.assert_called_once_with(
        schema_path or Endpoint.user_config_schema_path(ep_dir)
    )


@pytest.mark.parametrize("template_from", ["config", "param"])
@pytest.mark.parametrize("schema_from", ["config", "param"])
def test__do_render_user_config_parent_ep_overrides(
    mock_render_config_user_template,
    make_manager_endpoint_dir,
    template_from,
    schema_from,
):
    ep_dir: pathlib.Path = make_manager_endpoint_dir()

    template_config_path = ep_dir / "template.config"
    template_param_path = ep_dir / "template.param"
    schema_config_path = ep_dir / "schema.config"
    schema_param_path = ep_dir / "schema.param"

    template_config = "template from config"
    template_param = "template from param"
    schema_config = {"schema": "from config"}
    schema_param = {"schema": "from param"}

    template_config_path.write_text(template_config)
    template_param_path.write_text(template_param)
    schema_config_path.write_text(json.dumps(schema_config))
    schema_param_path.write_text(json.dumps(schema_param))

    with (ep_dir / "config.yaml").open("a") as f:
        f.write(
            f'\nuser_config_template_path: "{template_config_path}"\n'
            f'\nuser_config_schema_path: "{schema_config_path}"\n'
        )

    template_file = {
        "config": None,
        "param": template_param_path.open("r"),
    }.get(template_from)
    schema_file = {
        "config": None,
        "param": schema_param_path.open("r"),
    }.get(schema_from)

    _do_render_user_config(
        parent_ep_dir=ep_dir,
        template_file=template_file,
        user_schema_file=schema_file,
        user_options_file=None,
        parent_config_file=None,
        user_runtime_file=None,
        mapped_identity_file=None,
    )

    mock_render_config_user_template.assert_called_once()
    mock_render_kwargs = mock_render_config_user_template.call_args.kwargs
    assert mock_render_kwargs["user_config_template"] == (
        template_param if template_from == "param" else template_config
    )
    assert mock_render_kwargs["user_config_schema"] == (
        schema_param if schema_from == "param" else schema_config
    )


def test__do_render_user_config_generic_catch_all_exception(
    mocker,
    make_manager_endpoint_dir,
):
    ep_dir: pathlib.Path = make_manager_endpoint_dir()

    mock_render = mocker.patch(f"{_MOCK_BASE}render_config_user_template")
    mock_render.side_effect = RuntimeError("Something bad happened")

    with pytest.raises(ClickException) as pyt_exc:
        _do_render_user_config(
            parent_ep_dir=ep_dir,
            template_file=(ep_dir / "user_config_template.yaml.j2").open("r"),
            user_options_file=None,
            user_schema_file=None,
            parent_config_file=None,
            user_runtime_file=None,
            mapped_identity_file=None,
        )

    assert "Failed to render user configuration template." in str(pyt_exc.value)
    assert "Something bad happened" in str(pyt_exc.value)
    assert isinstance(pyt_exc.value.__cause__, RuntimeError)
