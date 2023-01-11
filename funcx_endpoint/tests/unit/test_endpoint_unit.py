import functools
import io
import json
import os
import pathlib
import random
import uuid
from collections import namedtuple
from unittest import mock

import pytest
import responses

from funcx_endpoint.endpoint import endpoint
from funcx_endpoint.endpoint.endpoint import Endpoint
from funcx_endpoint.endpoint.utils.config import Config

_mock_base = "funcx_endpoint.endpoint.endpoint."


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
            url="https://api2.funcx.org/v2/endpoints",
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
            url="https://api2.funcx.org/v2/endpoints",
            headers={"Content-Type": "application/json"},
            json={"error": "error msg"},
            status=status_code,
        )

    return create_response


@pytest.fixture
def mock_ep_data(fs):
    funcx_dir = pathlib.Path(endpoint._DEFAULT_FUNCX_DIR)
    ep = endpoint.Endpoint()

    ep_dir = funcx_dir / ep.name
    ep_dir.mkdir(parents=True, exist_ok=True)

    log_to_console = False
    no_color = True
    ep_conf = Config()
    yield ep, ep_dir, log_to_console, no_color, ep_conf


@pytest.fixture
def mock_ep_buf():
    buf = io.StringIO()
    # Endpoint.get_endpoint
    # ep = mocker.patch("funcx_endpoint.endpoint.endpoint.Endpoint.get_endpoints")
    Endpoint.get_endpoints = mock.Mock()
    Endpoint.get_endpoints.return_value = {}

    Endpoint.print_endpoint_table = functools.partial(
        Endpoint.print_endpoint_table, conf_dir="unused", ofile=buf
    )
    yield buf


@responses.activate
def test_start_endpoint(
    mocker,
    fs,
    randomstring,
    get_standard_funcx_client,
    register_endpoint_response,
    mock_ep_data,
):
    mock_fxc = get_standard_funcx_client()
    mock_log = mocker.patch(f"{_mock_base}log")
    mock_daemon = mocker.patch(f"{_mock_base}daemon")
    mock_epinterchange = mocker.patch(f"{_mock_base}EndpointInterchange")
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_fxc

    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())

    uname, pword = randomstring(), randomstring()
    register_endpoint_response(endpoint_id=ep_id, username=uname, password=pword)

    ep.start_endpoint(ep.name, ep_id, ep_conf, log_to_console, no_color)

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
    get_standard_funcx_client,
    mock_ep_data,
):
    mock_fxc = get_standard_funcx_client()
    mock_log = mocker.patch(f"{_mock_base}log")
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_fxc

    ep, _ep_dir, log_to_console, no_color, ep_conf = mock_ep_data

    register_endpoint_response(endpoint_id=other_endpoint_id)
    with pytest.raises(SystemExit) as pytest_exc:
        ep.start_endpoint(ep.name, endpoint_uuid, ep_conf, log_to_console, no_color)
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
    get_standard_funcx_client,
    mock_ep_data,
):
    """
    Check to ensure endpoint registration escalates up with API error
    """
    mock_fxc = get_standard_funcx_client()
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_fxc

    ep, funcx_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())
    register_endpoint_failure_response(endpoint_id=ep_id, status_code=423)
    with pytest.raises(SystemExit) as pytest_exc:
        ep.start_endpoint(ep.name, ep_id, ep_conf, log_to_console, no_color)
    assert pytest_exc.value.code == os.EX_UNAVAILABLE


@pytest.mark.parametrize("multi_tenant", [None, True, False])
@responses.activate
def test_register_endpoint_multi_tenant(
    mocker,
    fs,
    endpoint_uuid,
    register_endpoint_response,
    get_standard_funcx_client,
    randomstring,
    multi_tenant,
    mock_ep_data,
):
    mock_fxc = get_standard_funcx_client()
    mock_daemon = mocker.patch(f"{_mock_base}daemon")
    mock_epinterchange = mocker.patch(f"{_mock_base}EndpointInterchange")
    mocker.patch(f"{_mock_base}Endpoint.get_funcx_client").return_value = mock_fxc

    ep, ep_dir, log_to_console, no_color, ep_conf = mock_ep_data
    ep_id = str(uuid.uuid4())

    register_endpoint_response(endpoint_id=ep_id)
    if multi_tenant is not None:
        ep_conf.multi_tenant = multi_tenant

    ep.start_endpoint(ep.name, ep_id, ep_conf, log_to_console, no_color)

    assert mock_epinterchange.called
    assert mock_daemon.DaemonContext.called

    ep_json_p = ep_dir / "endpoint.json"
    assert ep_json_p.exists()

    request_body = json.loads(responses.calls[1].request.body)
    if multi_tenant is True:
        assert request_body["multi_tenant"] is True
    else:
        assert "multi_tenant" not in request_body


def test_list_endpoints_none_configured(mock_ep_buf):
    buf = mock_ep_buf
    Endpoint.print_endpoint_table()
    assert "No endpoints configured" in buf.getvalue()
    assert "Hint:" in buf.getvalue()
    assert "funcx-endpoint configure" in buf.getvalue()


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
    mock_shutil = mocker.patch("funcx_endpoint.endpoint.endpoint.shutil")
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
