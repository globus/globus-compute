import pathlib
import uuid
from unittest.mock import Mock, patch

import pytest
import responses
from click.testing import CliRunner
from funcx_common.response_errors.constants import HTTPStatusCode

import funcx.sdk.client
import funcx.sdk.login_manager
from funcx.sdk.web_client import FuncxWebClient
from funcx_endpoint.cli import _do_logout_endpoints, _do_stop_endpoint, app
from funcx_endpoint.endpoint import endpoint
from funcx_endpoint.endpoint.utils.config import Config


@pytest.fixture(autouse=True)
def patch_funcx_client(mocker):
    return mocker.patch("funcx_endpoint.endpoint.endpoint.FuncXClient")


def test_non_configured_endpoint(mocker):
    result = CliRunner().invoke(app, ["start", "newendpoint"])
    assert "newendpoint" in result.stdout
    assert "not configured" in result.stdout


@responses.activate
def test_start_endpoint_ep_locked(mocker, fs, randomstring, patch_funcx_client):
    # happy-path tested in tests/unit/test_endpoint_unit.py

    fx_addy = "http://api.funcx/"
    fxc = funcx.FuncXClient(
        funcx_service_address=fx_addy,
        do_version_check=False,
        login_manager=mocker.Mock(),
    )
    fxwc = FuncxWebClient(base_url=fx_addy)
    fxc.web_client = fxwc
    patch_funcx_client.return_value = fxc

    mock_log = mocker.patch("funcx_endpoint.endpoint.endpoint.log")
    reason_msg = randomstring()
    responses.add(
        responses.GET,
        fx_addy + "version",
        json={"api": "1.0.5", "min_ep_version": "1.0.5", "min_sdk_version": "1.0.5"},
        status=200,
    )
    responses.add(
        responses.POST,
        fx_addy + "endpoints",
        json={"reason": reason_msg},
        status=HTTPStatusCode.RESOURCE_LOCKED,
    )

    funcx_dir = pathlib.Path(endpoint._DEFAULT_FUNCX_DIR)
    ep = endpoint.Endpoint()

    (funcx_dir / ep.name).mkdir(parents=True, exist_ok=True)

    ep_id = str(uuid.uuid4())
    log_to_console = False
    no_color = True
    ep_conf = Config()

    with pytest.raises(SystemExit):
        ep.start_endpoint(ep.name, ep_id, ep_conf, log_to_console, no_color)
    args, kwargs = mock_log.warning.call_args
    assert "blocked" in args[0]
    assert reason_msg in args[0]


def test_endpoint_logout(monkeypatch):
    # not forced, and no running endpoints
    logout_true = Mock(return_value=True)
    logout_false = Mock(return_value=False)
    monkeypatch.setattr(funcx.sdk.login_manager.LoginManager, "logout", logout_true)
    success, msg = _do_logout_endpoints(
        False,
        running_endpoints={},
    )
    logout_true.assert_called_once()
    assert success

    logout_true.reset_mock()

    # forced, and no running endpoints
    success, msg = _do_logout_endpoints(
        True,
        running_endpoints={},
    )
    logout_true.assert_called_once()
    assert success

    one_running = {
        "default": {"status": "Running", "id": "123abcde-a393-4456-8de5-123456789abc"}
    }

    monkeypatch.setattr(funcx.sdk.login_manager.LoginManager, "logout", logout_false)
    # not forced, with running endpoint
    success, msg = _do_logout_endpoints(False, running_endpoints=one_running)
    logout_false.assert_not_called()
    assert not success

    logout_true.reset_mock()

    monkeypatch.setattr(funcx.sdk.login_manager.LoginManager, "logout", logout_true)
    # forced, with running endpoint
    success, msg = _do_logout_endpoints(True, running_endpoints=one_running)
    logout_true.assert_called_once()
    assert success


@patch(
    "funcx_endpoint.endpoint.endpoint.Endpoint.get_endpoint_id",
    return_value="abc-uuid",
)
@patch("funcx_endpoint.cli.get_config_dir", return_value=pathlib.Path("some_ep_dir"))
@patch("funcx_endpoint.cli.read_config")
@patch("funcx_endpoint.endpoint.endpoint.FuncXClient.lock_endpoint")
def test_endpoint_lock(mock_get_id, mock_get_conf, mock_get_fxc, mock_lock_endpoint):
    _do_stop_endpoint(name="abc-endpoint", remote=False)
    assert not mock_lock_endpoint.called
    _do_stop_endpoint(name="abc-endpoint", remote=True)
    assert mock_lock_endpoint.called
