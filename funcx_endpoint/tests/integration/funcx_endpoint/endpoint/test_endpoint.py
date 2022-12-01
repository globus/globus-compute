import pathlib
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

import funcx.sdk.client
import funcx.sdk.login_manager
from funcx_endpoint.cli import _do_logout_endpoints, _do_stop_endpoint, app

runner = CliRunner()


config_string = """
from funcx_endpoint.endpoint.utils.config import Config
from parsl.providers import LocalProvider

config = Config(
    scaling_enabled=True,
    provider=LocalProvider(
        init_blocks=1,
        min_blocks=1,
        max_blocks=1,
    ),
    funcx_service_address='https://api.funcx.org/v2'
)"""


@pytest.fixture(autouse=True)
def patch_funcx_client(mocker):
    return mocker.patch("funcx_endpoint.endpoint.endpoint.FuncXClient")


def test_non_configured_endpoint(mocker):
    result = runner.invoke(app, ["start", "newendpoint"])
    assert "newendpoint" in result.stdout
    assert "not configured" in result.stdout


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
@patch("funcx_endpoint.endpoint.endpoint.FuncXClient.lock_endpoint")
def test_endpoint_lock(mock_get_id, mock_get_conf, mock_lock_endpoint):
    _do_stop_endpoint(name="abc-endpoint", remote=False)
    assert not mock_lock_endpoint.called
    _do_stop_endpoint(name="abc-endpoint", remote=True)
    assert mock_lock_endpoint.called
