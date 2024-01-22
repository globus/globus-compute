from __future__ import annotations

import functools
import io
import json
import os
import pathlib
import random
import uuid
from collections import namedtuple
from contextlib import redirect_stdout
from http import HTTPStatus
from types import SimpleNamespace
from unittest import mock

import pytest
import responses
from globus_compute_endpoint.endpoint import endpoint
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.endpoint.config.default_config import (
    config as default_config,
)
from globus_compute_endpoint.endpoint.config.utils import (
    load_user_config_template,
    render_config_user_template,
    serialize_config,
)
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_sdk import GlobusAPIError, NetworkError
from pytest_mock import MockFixture

_mock_base = "globus_compute_endpoint.endpoint.endpoint."

# bloody line length ...
_whitespace_msg = "no whitespace (spaces, newlines, tabs), slashes, or prefixed '.'"


@pytest.fixture
def register_endpoint_response(endpoint_uuid):
    def create_response(
        endpoint_id=endpoint_uuid,
        queue_ttl_s=60,
        queue_arguments=None,
        queue_kwargs=None,
        rmq_fqdn="rabbitmq.fqdn",
        username="u",
        password="p",
    ):
        if queue_arguments is None:
            queue_arguments = {"x-expires": queue_ttl_s * 1000}
        if queue_kwargs is None:
            queue_kwargs: dict = {"durable": True, "arguments": queue_arguments}
        creds = ""
        if username and password:
            creds = f"{username}:{password}@"

        res_body = {
            "endpoint_id": endpoint_id,
            "task_queue_info": {
                "exchange_name": "tasks",
                "connection_url": f"amqp://{creds}{rmq_fqdn}",
                "args": queue_kwargs,
            },
            "result_queue_info": {
                "exchange_name": "results",
                "connection_url": f"amqp://{creds}{rmq_fqdn}",
                "args": queue_kwargs,
                "routing_key": f"{endpoint_uuid}.results",
            },
        }

        responses.add(
            method=responses.POST,
            url="https://compute.api.globus.org/v3/endpoints",
            headers={"Content-Type": "application/json"},
            json=res_body,
        )
        responses.add(
            method=responses.PUT,
            url=f"https://compute.api.globus.org/v3/endpoints/{endpoint_id}",
            headers={"Content-Type": "application/json"},
            json=res_body,
        )

    return create_response


@pytest.fixture
def register_endpoint_failure_response(endpoint_uuid):
    def create_response(endpoint_id=endpoint_uuid, status_code=200, msg="Error Msg"):
        responses.add(
            method=responses.POST,
            url="https://compute.api.globus.org/v3/endpoints",
            headers={"Content-Type": "application/json"},
            json={"error": msg},
            status=status_code,
        )
        responses.add(
            method=responses.PUT,
            url=f"https://compute.api.globus.org/v3/endpoints/{endpoint_id}",
            headers={"Content-Type": "application/json"},
            json={"error": msg},
            status=status_code,
        )

    return create_response


@pytest.fixture
def mock_ep_data(fs):
    ep = endpoint.Endpoint()
    ep_dir = pathlib.Path("/some/path/mock_endpoint")
    ep_dir.mkdir(parents=True, exist_ok=True)
    log_to_console = False
    no_color = True
    ep_conf = Config()
    yield ep, ep_dir, log_to_console, no_color, ep_conf


@pytest.fixture
def mock_ep_buf():
    buf = io.StringIO()
    Endpoint.get_endpoints = mock.Mock()
    Endpoint.get_endpoints.return_value = {}

    Endpoint.print_endpoint_table = functools.partial(
        Endpoint.print_endpoint_table, conf_dir="unused", ofile=buf
    )
    yield buf


@pytest.fixture
def umask():
    orig_umask = os.umask(0)
    os.umask(orig_umask)

    def _wrapped_umask(new_umask: int | None) -> int:
        if new_umask is None:
            return orig_umask
        return os.umask(new_umask)

    yield _wrapped_umask

    os.umask(orig_umask)


@pytest.fixture
def mock_reg_info():
    yield {
        "endpoint_id": str(uuid.uuid4()),
        "task_queue_info": {"connection_url": "amqp://some.domain:1234"},
        "result_queue_info": {"connection_url": "amqp://some.domain"},
    }


@responses.activate
def test_start_endpoint(
    mocker,
    fs,
    randomstring,
    get_standard_compute_client,
    register_endpoint_response,
    mock_ep_data,
):
    mock_gcc = get_standard_compute_client()
    mock_log = mocker.patch(f"{_mock_base}log")
    mock_daemon = mocker.patch(f"{_mock_base}daemon")
    mock_epinterchange = mocker.patch(f"{_mock_base}EndpointInterchange")
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc

    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())

    uname, pword = randomstring(), randomstring()
    register_endpoint_response(endpoint_id=ep_id, username=uname, password=pword)

    ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info={})

    assert mock_epinterchange.called
    assert mock_daemon.DaemonContext.called

    ep_json_p = ep_dir / "endpoint.json"
    assert ep_json_p.exists()
    ep_data = json.load(ep_json_p.open())
    assert ep_data["endpoint_id"] == ep_id
    assert uname not in str(ep_data)
    assert pword not in str(ep_data)

    debug_args = str([str((a, k)) for a, k in mock_log.debug.call_args_list])
    assert "Registration information: " in debug_args
    assert uname not in debug_args
    assert pword not in debug_args


@responses.activate
def test_start_endpoint_network_error(
    mocker: MockFixture,
    fs,
    randomstring,
    get_standard_compute_client,
    register_endpoint_response,
    mock_ep_data,
):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_uuid_str = str(uuid.uuid4())

    uname, pword = randomstring(), randomstring()
    register_endpoint_response(endpoint_id=ep_uuid_str, username=uname, password=pword)

    mock_gcc = get_standard_compute_client()
    mocker.patch.object(
        mock_gcc, "register_endpoint", side_effect=NetworkError("foo", Exception)
    )
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc
    mock_log = mocker.patch(f"{_mock_base}log")

    f = io.StringIO()
    f.isatty = lambda: True
    with redirect_stdout(f):
        with pytest.raises(SystemExit) as pytest_exc:
            ep.start_endpoint(
                ep_dir, ep_uuid_str, ep_conf, log_to_console, no_color, reg_info={}
            )

    assert pytest_exc.value.code == os.EX_TEMPFAIL
    assert "exception while attempting" in mock_log.exception.call_args[0][0]
    assert "unable to reach the Globus Compute" in mock_log.critical.call_args[0][0]
    assert "unable to reach the Globus Compute" in f.getvalue()  # stdout


@responses.activate
def test_delete_endpoint_network_error(
    mocker: MockFixture,
    fs,
    randomstring,
    get_standard_compute_client,
    register_endpoint_response,
    mock_ep_data,
):
    ep, ep_dir, _, _, ep_conf = mock_ep_data
    ep_uuid_str = str(uuid.uuid4())

    uname, pword = randomstring(), randomstring()
    register_endpoint_response(endpoint_id=ep_uuid_str, username=uname, password=pword)

    mock_gcc = get_standard_compute_client()
    mocker.patch.object(
        mock_gcc, "delete_endpoint", side_effect=NetworkError("foo", Exception)
    )
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc
    mocker.patch(f"{_mock_base}Endpoint.get_endpoint_id").return_value = ep_uuid_str
    mock_log = mocker.patch(f"{_mock_base}log")

    f = io.StringIO()
    with redirect_stdout(f):
        with pytest.raises(SystemExit) as pytest_exc:
            ep.delete_endpoint(ep_dir, ep_conf)

    assert pytest_exc.value.code == os.EX_TEMPFAIL
    assert "could not be deleted from the web" in mock_log.warning.call_args[0][0]
    assert "unable to reach the Globus Compute" in mock_log.critical.call_args[0][0]
    assert "unable to reach the Globus Compute" in f.getvalue()  # stdout


@responses.activate
def test_register_endpoint_invalid_response(
    mocker,
    fs,
    endpoint_uuid,
    other_endpoint_id,
    register_endpoint_response,
    get_standard_compute_client,
    mock_ep_data,
):
    mock_gcc = get_standard_compute_client()
    mock_log = mocker.patch(f"{_mock_base}log")
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc

    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data

    responses.add(
        method=responses.PUT,
        url=f"https://compute.api.globus.org/v3/endpoints/{endpoint_uuid}",
        headers={"Content-Type": "application/json"},
        json={"endpoint_id": other_endpoint_id},
    )

    with pytest.raises(SystemExit) as pytest_exc:
        ep.start_endpoint(
            ep_dir, endpoint_uuid, ep_conf, log_to_console, no_color, reg_info={}
        )
    assert pytest_exc.value.code == os.EX_SOFTWARE
    assert "mismatched endpoint id" in mock_log.error.call_args[0][0]
    assert "Expected" in mock_log.error.call_args[0][0]
    assert "received" in mock_log.error.call_args[0][0]
    assert endpoint_uuid in mock_log.error.call_args[0][0]
    assert other_endpoint_id in mock_log.error.call_args[0][0]


@pytest.mark.parametrize(
    "exit_code,status_code",
    (
        (os.EX_UNAVAILABLE, HTTPStatus.CONFLICT),
        (os.EX_UNAVAILABLE, HTTPStatus.LOCKED),
        (os.EX_UNAVAILABLE, HTTPStatus.NOT_FOUND),
        (os.EX_DATAERR, HTTPStatus.BAD_REQUEST),
        (os.EX_DATAERR, HTTPStatus.UNPROCESSABLE_ENTITY),
        ("Error", 418),  # IM_A_TEAPOT
    ),
)
@responses.activate
def test_register_endpoint_blocked(
    mocker,
    fs,
    register_endpoint_failure_response,
    get_standard_compute_client,
    mock_ep_data,
    randomstring,
    exit_code,
    status_code,
):
    """
    Check to ensure endpoint registration escalates up with API error
    """
    mock_log = mocker.patch(f"{_mock_base}log")
    mock_gcc = get_standard_compute_client()
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc

    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())
    some_err = randomstring()
    register_endpoint_failure_response(
        endpoint_id=ep_id,
        status_code=status_code,
        msg=some_err,
    )

    f = io.StringIO()
    f.isatty = lambda: True
    with redirect_stdout(f):
        with pytest.raises((GlobusAPIError, SystemExit)) as pytexc:
            ep.start_endpoint(
                ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info={}
            )
        stdout_msg = f.getvalue()

    assert mock_log.warning.called
    a, *_ = mock_log.warning.call_args
    assert some_err in str(a), "Expected upstream response still shared"

    assert some_err in stdout_msg, f"Expecting error message in stdout ({stdout_msg})"
    assert pytexc.value.code == exit_code, "Expecting meaningful exit code"

    if exit_code == "Error":
        # The other route tests SystemExit; nominally this route is an unhandled
        # traceback -- good.  We should _not_ blanket hide all exceptions.
        assert pytexc.value.http_status == status_code


def test_register_endpoint_already_active(
    mocker,
    fs,
    get_standard_compute_client,
    mock_ep_data,
):
    """
    Check to ensure endpoint already active message prints to console
    """
    mock_gcc = get_standard_compute_client()
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc

    pid_active = {
        "exists": True,
        "active": True,
    }
    mocker.patch(f"{_mock_base}Endpoint.check_pidfile").return_value = pid_active

    f = io.StringIO()
    f.isatty = lambda: True

    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())
    with redirect_stdout(f):
        with pytest.raises(SystemExit) as pytest_exc:
            ep.start_endpoint(
                ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info={}
            )
        assert "is already active" in f.getvalue()
        assert pytest_exc.value.code == -1


@pytest.mark.parametrize("multi_user", [None, True, False])
@responses.activate
def test_register_endpoint_is_not_multiuser(
    mocker,
    fs,
    endpoint_uuid,
    register_endpoint_response,
    get_standard_compute_client,
    randomstring,
    multi_user,
    mock_ep_data,
):
    mock_gcc = get_standard_compute_client()
    mock_daemon = mocker.patch(f"{_mock_base}daemon")
    mock_epinterchange = mocker.patch(f"{_mock_base}EndpointInterchange")
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc

    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())

    register_endpoint_response(endpoint_id=ep_id)
    if multi_user is not None:
        ep_conf.multi_user = multi_user

    ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info={})

    assert mock_epinterchange.called
    assert mock_daemon.DaemonContext.called

    ep_json_p = ep_dir / "endpoint.json"
    assert ep_json_p.exists()

    request_body = json.loads(responses.calls[1].request.body)
    assert "multi_user" not in request_body, "endpoint.py is single-user logic only"


def test_list_endpoints_none_configured(mock_ep_buf):
    buf = mock_ep_buf
    Endpoint.print_endpoint_table()
    assert "No endpoints configured" in buf.getvalue()
    assert "Hint:" in buf.getvalue()
    assert "globus-compute-endpoint configure" in buf.getvalue()


def test_list_endpoints_no_id_yet(mock_ep_buf, randomstring):
    buf = mock_ep_buf
    expected_col_length = random.randint(2, 30)
    Endpoint.get_endpoints.return_value = {
        "default": {"status": randomstring(length=expected_col_length), "id": None}
    }
    Endpoint.print_endpoint_table()
    assert Endpoint.get_endpoints.return_value["default"]["status"] in buf.getvalue()
    assert "| Endpoint ID |" in buf.getvalue(), "Expecting column shrinks to size"


@pytest.mark.parametrize("term_size", ((30, 5), (50, 5), (67, 5), (72, 5), (120, 5)))
def test_list_endpoints_long_names_wrapped(
    mock_ep_buf, mocker, term_size, randomstring
):
    buf = mock_ep_buf
    tsize = namedtuple("terminal_size", ["columns", "lines"])(*term_size)
    mock_shutil = mocker.patch("globus_compute_endpoint.endpoint.endpoint.shutil")
    mock_shutil.get_terminal_size.return_value = tsize

    def rand_length_str(min_=2, max_=30):
        return randomstring(length=random.randint(min_, max_))

    expected_data = {
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": None},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
    }
    Endpoint.get_endpoints.return_value = expected_data

    Endpoint.print_endpoint_table()

    for ep_name, ep in expected_data.items():
        assert ep["status"] in buf.getvalue(), "expected no wrapping of status"
        assert str(ep["id"]) in buf.getvalue(), "expected no wrapping of id"
        assert ep_name not in buf.getvalue(), "expected only name column is wrapped"


@pytest.mark.parametrize(
    "pid_info",
    [
        [False, None, False, False],
        [True, "", True, False],
        [True, "123", True, False],
    ],
)
def test_pid_file_check(pid_info, fs):
    has_file, pid_content, should_exist, should_active = pid_info

    ep_dir = pathlib.Path(".")
    if has_file:
        (ep_dir / "daemon.pid").write_text(pid_content)

    pid_status = Endpoint.check_pidfile(ep_dir)
    assert should_exist == pid_status["exists"]
    assert should_active == pid_status["active"]


def test_endpoint_get_metadata(mocker):
    mock_data = {
        "endpoint_version": "106.7",
        "hostname": "oneohtrix.never",
        "local_user": "daniel",
    }

    mocker.patch(
        "globus_compute_endpoint.endpoint.endpoint.__version__",
        mock_data["endpoint_version"],
    )

    mock_fqdn = mocker.patch("globus_compute_endpoint.endpoint.endpoint.socket.getfqdn")
    mock_fqdn.return_value = mock_data["hostname"]

    mock_pwuid = mocker.patch("globus_compute_endpoint.endpoint.endpoint.pwd.getpwuid")
    mock_pwuid.return_value = SimpleNamespace(pw_name=mock_data["local_user"])

    meta = Endpoint.get_metadata(default_config)

    for k, v in mock_data.items():
        assert meta[k] == v

    config = meta["config"]
    assert "funcx_service_address" in config
    assert len(config["executors"]) == 1
    assert config["executors"][0]["type"] == "GlobusComputeEngine"
    assert config["executors"][0]["executor"]["provider"]["type"] == "LocalProvider"


@pytest.mark.parametrize("env", [None, "blar", "local", "production"])
def test_endpoint_sets_process_title(
    mocker, fs, randomstring, mock_ep_data, env, mock_reg_info
):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())
    ep_conf.environment = env

    orig_proc_title = randomstring()

    mock_gcc = mocker.Mock()
    mock_gcc.register_endpoint.return_value = {**mock_reg_info, "endpoint_id": ep_id}
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client", return_value=mock_gcc)

    mock_spt = mocker.patch(f"{_mock_base}setproctitle")
    mock_spt.getproctitle.return_value = orig_proc_title
    mock_spt.setproctitle.side_effect = StopIteration("Sentinel")

    with pytest.raises(StopIteration, match="Sentinel"):
        ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info={})

    a, _k = mock_spt.setproctitle.call_args
    assert a[0].startswith(
        "Globus Compute Endpoint"
    ), "Expect easily identifiable process name"
    assert f"{ep_id}, {ep_dir.name}" in a[0], "Expect easily match process to ep conf"
    if not env:
        assert " - " not in a[0], "Default is not 'do not show env' for prod"
    else:
        assert f" - {env}" in a[0], "Expected environment name in title"
    assert a[0].endswith(f"[{orig_proc_title}]"), "Save original cmdline for debugging"


@pytest.mark.parametrize("port", [random.randint(0, 65535)])
def test_endpoint_respects_port(mocker, fs, mock_ep_data, port):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())
    ep_conf.amqp_port = port

    tq_url = "amqp://some.domain:1234"
    rq_url = "amqp://some.domain"

    mock_reg_info = {
        "endpoint_id": ep_id,
        "task_queue_info": {"connection_url": tq_url},
        "result_queue_info": {"connection_url": rq_url},
    }

    mock_update_url_port = mocker.patch(f"{_mock_base}update_url_port")
    mock_update_url_port.side_effect = (None, StopIteration("Sentinel"))

    with pytest.raises(StopIteration, match="Sentinel"):
        ep.start_endpoint(
            ep_dir, ep_id, ep_conf, log_to_console, no_color, mock_reg_info
        )

    assert mock_update_url_port.call_args_list[0] == ((tq_url, port),)
    assert mock_update_url_port.call_args_list[1] == ((rq_url, port),)


def test_endpoint_needs_no_client_if_reg_info(
    mocker, fs, randomstring, mock_ep_data, mock_reg_info
):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())

    mock_gcc = mocker.Mock()
    mock_gcc.register_endpoint.return_value = {**mock_reg_info, "endpoint_id": ep_id}
    mock_get_compute_client = mocker.patch(
        f"{_mock_base}Endpoint.get_funcx_client", return_value=mock_gcc
    )
    mock_daemon = mocker.patch(f"{_mock_base}daemon")
    mock_epinterchange = mocker.patch(f"{_mock_base}EndpointInterchange")

    reg_info = {**mock_reg_info, "endpoint_id": ep_id}
    ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info)

    assert mock_epinterchange.called, "Has registration, should start."
    assert mock_daemon.DaemonContext.called
    assert not mock_get_compute_client.called, "No need for FXClient!"

    reg_info.clear()
    ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info)
    assert mock_epinterchange.called, "Has registration, should start."
    assert mock_daemon.DaemonContext.called
    assert mock_get_compute_client.called, "Need registration info, need FXClient"


def test_endpoint_sets_owner_only_access(tmp_path, umask):
    umask(0)
    ep_dir = tmp_path / "new_endpoint_dir"
    Endpoint.init_endpoint_dir(ep_dir)

    # assert ep_dir.stat() & 0o77 == 0, "Expected no group or other access"
    assert ep_dir.stat().st_mode & 0o777 == 0o700, "Expected user-only access"


def test_endpoint_config_handles_umask_gracefully(tmp_path, umask):
    umask(0o777)  # No access whatsoever
    ep_dir = tmp_path / "new_endpoint_dir"
    Endpoint.init_endpoint_dir(ep_dir)

    assert ep_dir.stat().st_mode & 0o777 == 0o300, "Should honor user-read bit"
    ep_dir.chmod(0o700)  # necessary for test to cleanup after itself


def test_mu_endpoint_user_ep_yamls_world_readable(tmp_path):
    ep_dir = tmp_path / "new_endpoint_dir"
    Endpoint.init_endpoint_dir(ep_dir, multi_user=True)

    user_tmpl_path = Endpoint.user_config_template_path(ep_dir)
    user_env_path = Endpoint._user_environment_path(ep_dir)

    assert user_env_path != user_tmpl_path, "Dev typo while developing"
    for p in (user_tmpl_path, user_env_path):
        assert p.exists()
        assert p.stat().st_mode & 0o444 == 0o444, "Minimum world readable"
    assert ep_dir.stat().st_mode & 0o111 == 0o111, "Minimum world executable"


def test_mu_endpoint_user_ep_sensible_default(tmp_path):
    ep_dir = tmp_path / "new_endpoint_dir"
    Endpoint.init_endpoint_dir(ep_dir, multi_user=True)

    tmpl_str, schema = load_user_config_template(ep_dir)
    # Doesn't crash; loads yaml, jinja template has defaults
    render_config_user_template(tmpl_str, schema, {})


def test_always_prints_endpoint_id_to_terminal(mocker, mock_ep_data, mock_reg_info):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())

    mocker.patch(f"{_mock_base}daemon")
    mocker.patch(f"{_mock_base}EndpointInterchange")
    mock_dup2 = mocker.patch(f"{_mock_base}os.dup2")
    mock_dup2.return_value = 0
    mock_sys = mocker.patch(f"{_mock_base}sys")

    expected_text = f"Starting endpoint; registered ID: {ep_id}"

    reg_info = {**mock_reg_info, "endpoint_id": ep_id}

    mock_sys.stdout.isatty.return_value = True
    ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info)

    assert mock_sys.stdout.write.called
    assert not mock_sys.stderr.write.called
    assert any(expected_text == a[0] for a, _ in mock_sys.stdout.write.call_args_list)

    mock_sys.reset_mock()
    mock_sys.stdout.isatty.return_value = False
    mock_sys.stderr.isatty.return_value = True
    ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info)

    assert not mock_sys.stdout.write.called
    assert mock_sys.stderr.write.called
    assert any(expected_text == a[0] for a, _ in mock_sys.stderr.write.call_args_list)
    mock_sys.reset_mock()
    mock_sys.stderr.isatty.return_value = False
    ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info)

    assert not mock_sys.stdout.write.called
    assert not mock_sys.stderr.write.called


def test_serialize_config_field_types():
    ep_config = Config()

    ep_config._hidden_attr = "123"
    ep_config.rando_attr = "howdy"
    ep_config.allowed_functions = ["a", "b", "c"]
    ep_config.heartbeat_threshold = float("inf")

    class Foo:
        def __init__(self, foo):
            self._foo = foo

        @property
        def foo(self):
            return self._foo

    # Testing support for properties
    ep_config.environment = Foo("bar")

    result = serialize_config(ep_config)

    # Objects with a __dict__ attr are expanded
    assert "type" in result["executors"][0]["executor"]["provider"]

    # Only constructor parameters should be included
    assert "_hidden_attr" not in result
    assert "rando_attr" not in result

    # Most values should retain their type
    assert isinstance(result["allowed_functions"], list)
    assert len(result["allowed_functions"]) == 3
    assert isinstance(result["heartbeat_period"], int)
    assert isinstance(result["detach_endpoint"], bool)
    assert result["environment"]["foo"] == "bar"

    # Others should not
    assert isinstance(result["heartbeat_threshold"], str)


@pytest.mark.parametrize(
    "ep_path_name",
    (
        (True, "nice_normal_name", None, None),
        (True, "12345AnotherValid_name", None, None),
        (False, "", False, "Received no endpoint name"),
        (True, "a" * 128, None, None),
        (False, "a" * 129, False, "must be less than 129 characters (length: "),
        (False, "../ep_name", True, "no '..' or '/' characters"),
        (False, "./ep_name", True, "no '..' or '/' characters"),
        (False, "/ep_name", True, "no '..' or '/' characters"),
        (False, ".initial_dot_hidden_directories_disallowed", True, _whitespace_msg),
        (False, "contains...\r...other_whitespace", True, _whitespace_msg),
        (False, "contains...\v...other_whitespace", True, _whitespace_msg),
        (False, "contains...\t...other_whitespace", True, _whitespace_msg),
        (False, "contains...\n...other_whitespace", True, _whitespace_msg),
        (False, " NoSpaces", True, _whitespace_msg),
        (False, "No Spaces", True, _whitespace_msg),
        (False, "NoSpaces ", True, _whitespace_msg),
        (False, "NoEsc\\apes", True, _whitespace_msg),
        (False, "No'singlequotes'", True, _whitespace_msg),
        (False, 'No"doublequotes"', True, _whitespace_msg),
    ),
)
def test_validate_endpoint_name(ep_path_name):
    is_valid, name, shows_before_after, err_msg = ep_path_name
    if not is_valid:
        with pytest.raises(ValueError) as pyt_exc:
            Endpoint.validate_endpoint_name(name)
        assert err_msg in str(pyt_exc.value)
        if shows_before_after:
            assert "Requested: " in str(pyt_exc.value), "Should show what was received"
            assert name in str(pyt_exc.value), "Should show what was received"
            assert "Reduced to: " in str(pyt_exc.value), "Should show potential fix"
    else:
        Endpoint.validate_endpoint_name(name)


_test_get_endpoint_dir_by_uuid__data = [
    ("foo", str(uuid.uuid4()), True),
    ("non-existent", str(uuid.uuid4()), False),
]


@pytest.mark.parametrize("name,uuid,exists", _test_get_endpoint_dir_by_uuid__data)
def test_get_endpoint_dir_by_uuid(tmp_path, name, uuid, exists):
    gc_conf_dir = tmp_path / ".globus_compute"
    gc_conf_dir.mkdir()
    for n, u, e in _test_get_endpoint_dir_by_uuid__data:
        if not e:
            continue
        ep_conf_dir = gc_conf_dir / n
        ep_conf_dir.mkdir()
        ep_json = ep_conf_dir / "endpoint.json"
        ep_json.write_text(json.dumps({"endpoint_id": u}))
        # dummy config.yaml so that Endpoint._get_ep_dirs finds this
        (ep_conf_dir / "config.yaml").write_text("")

    result = Endpoint.get_endpoint_dir_by_uuid(gc_conf_dir, uuid)

    if exists:
        assert result is not None
    else:
        assert result is None


@pytest.mark.parametrize("json_exists", [True, False])
def test_get_endpoint_id(tmp_path: pathlib.Path, json_exists: bool):
    ep_uuid_str = str(uuid.uuid4())
    if json_exists:
        ep_json = tmp_path / "endpoint.json"
        ep_json.write_text(json.dumps({"endpoint_id": ep_uuid_str}))

    ret = Endpoint.get_endpoint_id(endpoint_dir=tmp_path)

    if json_exists:
        assert ret == ep_uuid_str
    else:
        assert ret is None


def test_handles_provided_endpoint_id_no_json(
    mocker: MockFixture,
    mock_ep_data: tuple[Endpoint, pathlib.Path, bool, bool, Config],
    mock_reg_info: dict,
):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_uuid_str = str(uuid.uuid4())

    mocker.patch(f"{_mock_base}daemon")
    mocker.patch(f"{_mock_base}EndpointInterchange")

    mock_gcc = mocker.Mock()
    mock_gcc.register_endpoint.return_value = {
        **mock_reg_info,
        "endpoint_id": ep_uuid_str,
    }
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc

    ep.start_endpoint(
        ep_dir, ep_uuid_str, ep_conf, log_to_console, no_color, reg_info={}
    )

    _a, k = mock_gcc.register_endpoint.call_args
    assert k["endpoint_id"] == ep_uuid_str


def test_handles_provided_endpoint_id_with_json(
    mocker: MockFixture,
    mock_ep_data: tuple[Endpoint, pathlib.Path, bool, bool, Config],
    mock_reg_info: dict,
):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_uuid_str = str(uuid.uuid4())
    provided_ep_uuid_str = str(uuid.uuid4())

    ep_json = ep_dir / "endpoint.json"
    ep_json.write_text(json.dumps({"endpoint_id": ep_uuid_str}))

    mocker.patch(f"{_mock_base}daemon")
    mocker.patch(f"{_mock_base}EndpointInterchange")

    mock_gcc = mocker.Mock()
    mock_gcc.register_endpoint.return_value = {
        **mock_reg_info,
        "endpoint_id": ep_uuid_str,
    }
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc

    ep.start_endpoint(
        ep_dir, provided_ep_uuid_str, ep_conf, log_to_console, no_color, reg_info={}
    )

    _a, k = mock_gcc.register_endpoint.call_args
    assert k["endpoint_id"] == ep_uuid_str
