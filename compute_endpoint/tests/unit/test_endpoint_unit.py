from __future__ import annotations

import functools
import io
import json
import os
import pathlib
import random
import uuid
from collections import namedtuple
from types import SimpleNamespace
from unittest import mock

import pytest
import responses
from globus_compute_endpoint.endpoint import endpoint
from globus_compute_endpoint.endpoint.default_config import config as default_config
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_endpoint.endpoint.utils.config import Config

_mock_base = "globus_compute_endpoint.endpoint.endpoint."


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
        responses.add(
            method=responses.POST,
            url="https://compute.api.globus.org/v2/endpoints",
            headers={"Content-Type": "application/json"},
            json={
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
            },
        )

    return create_response


@pytest.fixture
def register_endpoint_failure_response(endpoint_uuid):
    def create_response(endpoint_id=endpoint_uuid, status_code=200):
        responses.add(
            method=responses.POST,
            url="https://compute.api.globus.org/v2/endpoints",
            headers={"Content-Type": "application/json"},
            json={"error": "error msg"},
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

    register_endpoint_response(endpoint_id=other_endpoint_id)
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


@responses.activate
def test_register_endpoint_locked_error(
    mocker,
    fs,
    register_endpoint_failure_response,
    get_standard_compute_client,
    mock_ep_data,
):
    """
    Check to ensure endpoint registration escalates up with API error
    """
    mock_gcc = get_standard_compute_client()
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc

    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())
    register_endpoint_failure_response(endpoint_id=ep_id, status_code=423)
    with pytest.raises(SystemExit) as pytest_exc:
        ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info={})
    assert pytest_exc.value.code == os.EX_UNAVAILABLE


@pytest.mark.parametrize("multi_tenant", [None, True, False])
@responses.activate
def test_register_endpoint_is_not_multitenant(
    mocker,
    fs,
    endpoint_uuid,
    register_endpoint_response,
    get_standard_compute_client,
    randomstring,
    multi_tenant,
    mock_ep_data,
):
    mock_gcc = get_standard_compute_client()
    mock_daemon = mocker.patch(f"{_mock_base}daemon")
    mock_epinterchange = mocker.patch(f"{_mock_base}EndpointInterchange")
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_gcc

    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())

    register_endpoint_response(endpoint_id=ep_id)
    if multi_tenant is not None:
        ep_conf.multi_tenant = multi_tenant

    ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info={})

    assert mock_epinterchange.called
    assert mock_daemon.DaemonContext.called

    ep_json_p = ep_dir / "endpoint.json"
    assert ep_json_p.exists()

    request_body = json.loads(responses.calls[1].request.body)
    assert "multi_tenant" not in request_body, "endpoint.py is single-tenant logic only"


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

    assert meta["ip_address"] is None
    assert meta["sdk_version"] is None

    config = meta["config"]
    assert "funcx_service_address" in config
    assert len(config["executors"]) == 1
    assert config["executors"][0]["_type"] == "HighThroughputExecutor"
    assert config["executors"][0]["provider"]["_type"] == "LocalProvider"


@pytest.mark.parametrize("env", [None, "blar", "local", "production"])
def test_endpoint_sets_process_title(mocker, fs, randomstring, mock_ep_data, env):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())
    ep_conf.environment = env

    orig_proc_title = randomstring()

    mock_gcc = mocker.Mock()
    mock_gcc.register_endpoint.return_value = {"endpoint_id": ep_id}
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


def test_endpoint_needs_no_client_if_reg_info(mocker, fs, randomstring, mock_ep_data):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())

    mock_gcc = mocker.Mock()
    mock_gcc.register_endpoint.return_value = {"endpoint_id": ep_id}
    mock_get_compute_client = mocker.patch(
        f"{_mock_base}Endpoint.get_funcx_client", return_value=mock_gcc
    )
    mock_daemon = mocker.patch(f"{_mock_base}daemon")
    mock_epinterchange = mocker.patch(f"{_mock_base}EndpointInterchange")

    reg_info = {"endpoint_id": ep_id}
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


def test_always_prints_endpoint_id_to_terminal(mocker, mock_ep_data):
    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())

    mocker.patch(f"{_mock_base}daemon")
    mocker.patch(f"{_mock_base}EndpointInterchange")
    mock_dup2 = mocker.patch(f"{_mock_base}os.dup2")
    mock_dup2.return_value = 0
    mock_sys = mocker.patch(f"{_mock_base}sys")

    expected_text = f"Starting endpoint; registered ID: {ep_id}"

    reg_info = {"endpoint_id": ep_id}

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
