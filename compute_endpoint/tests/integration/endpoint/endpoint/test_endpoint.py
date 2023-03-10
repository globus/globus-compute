import os.path
import pathlib
import uuid
from unittest.mock import Mock, patch

import globus_compute_sdk.sdk.client
import globus_compute_sdk.sdk.login_manager
import pytest
import responses
from click import ClickException
from click.testing import CliRunner
from globus_compute_endpoint.cli import (
    _do_logout_endpoints,
    _do_stop_endpoint,
    _upgrade_funcx_imports_in_config,
    app,
)
from globus_compute_endpoint.endpoint import endpoint
from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_sdk.sdk.web_client import WebClient


@pytest.fixture(autouse=True)
def patch_funcx_client(mocker):
    return mocker.patch("globus_compute_endpoint.endpoint.endpoint.Client")


def test_non_configured_endpoint(mocker):
    result = CliRunner().invoke(app, ["start", "newendpoint"])
    assert "newendpoint" in result.stdout
    assert "not configured" in result.stdout


@pytest.mark.parametrize("status_code", [409, 410, 423])
@responses.activate
def test_start_endpoint_blocked(
    mocker, fs, randomstring, patch_funcx_client, status_code
):
    # happy-path tested in tests/unit/test_endpoint_unit.py

    fx_addy = "http://api.funcx/"
    gcc = globus_compute_sdk.Client(
        funcx_service_address=fx_addy,
        do_version_check=False,
        login_manager=mocker.Mock(),
    )
    fxwc = WebClient(base_url=fx_addy)
    gcc.web_client = fxwc
    patch_funcx_client.return_value = gcc

    mock_log = mocker.patch("globus_compute_endpoint.endpoint.endpoint.log")
    reason_msg = randomstring()
    responses.add(
        responses.GET,
        fx_addy + "version",
        json={"api": "1.0.5", "min_ep_version": "1.0.5", "min_sdk_version": "0.0.2a0"},
        status=200,
    )
    responses.add(
        responses.POST,
        fx_addy + "endpoints",
        json={"reason": reason_msg},
        status=status_code,
    )

    ep_dir = pathlib.Path("/some/path/some_endpoint_name")
    ep_dir.mkdir(parents=True, exist_ok=True)

    ep_id = str(uuid.uuid4())
    log_to_console = False
    no_color = True
    ep_conf = Config()

    ep = endpoint.Endpoint()
    with pytest.raises(SystemExit):
        ep.start_endpoint(ep_dir, ep_id, ep_conf, log_to_console, no_color, reg_info={})
    args, kwargs = mock_log.warning.call_args
    assert "blocked" in args[0]
    assert reason_msg in args[0]


def test_endpoint_logout(monkeypatch):
    # not forced, and no running endpoints
    logout_true = Mock(return_value=True)
    logout_false = Mock(return_value=False)
    monkeypatch.setattr(
        globus_compute_sdk.sdk.login_manager.LoginManager, "logout", logout_true
    )
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

    monkeypatch.setattr(
        globus_compute_sdk.sdk.login_manager.LoginManager, "logout", logout_false
    )
    # not forced, with running endpoint
    success, msg = _do_logout_endpoints(False, running_endpoints=one_running)
    logout_false.assert_not_called()
    assert not success

    logout_true.reset_mock()

    monkeypatch.setattr(
        globus_compute_sdk.sdk.login_manager.LoginManager, "logout", logout_true
    )
    # forced, with running endpoint
    success, msg = _do_logout_endpoints(True, running_endpoints=one_running)
    logout_true.assert_called_once()
    assert success


@patch(
    "globus_compute_endpoint.endpoint.endpoint.Endpoint.get_endpoint_id",
    return_value="abc-uuid",
)
@patch(
    "globus_compute_endpoint.cli.get_config_dir",
    return_value=pathlib.Path("some_ep_dir"),
)
@patch("globus_compute_endpoint.cli.read_config")
@patch("globus_compute_endpoint.endpoint.endpoint.Client.stop_endpoint")
def test_stop_remote_endpoint(
    mock_get_id, mock_get_conf, mock_get_gcc, mock_stop_endpoint
):
    _do_stop_endpoint(name="abc-endpoint", remote=False)
    assert not mock_stop_endpoint.called
    _do_stop_endpoint(name="abc-endpoint", remote=True)
    assert mock_stop_endpoint.called


@patch(
    "globus_compute_endpoint.endpoint.endpoint.Endpoint.get_endpoint_id",
    return_value="abc-uuid",
)
@patch(
    "globus_compute_endpoint.cli.get_config_dir",
    return_value=pathlib.Path(),
)
@pytest.mark.parametrize(
    "cur_config",
    [
        [
            ("abc\n" "bcd" "cef"),
            False,
            False,
            True,
            False,
        ],
        [
            ("abc\n" "bcd" "cef"),
            False,
            False,
            True,
            True,
        ],
        [
            (
                "from funcx_endpoint.endpoint.utils.config import Config\n"
                "from funcx_endpoint.executors import HighThroughputExecutor\n"
                "from parsl.providers import LocalProvider\n"
                "\n"
                "config = Config(\n"
                "    executors=[\n"
                "        HighThroughputExecutor(\n"
                "            provider=LocalProvider(\n"
                "                init_blocks=1,\n"
                "                min_blocks=0,\n"
                "                max_blocks=1,\n"
                "),\n"
            ),
            False,
            True,
            False,
            False,
        ],
        [
            (
                "from funcx_endpoint.endpoint.utils.config import Config\n"
                "from funcx_endpoint.executors import HighThroughputExecutor\n"
                "from parsl.providers import LocalProvider\n"
            ),
            False,
            True,
            False,
            True,
        ],
        [
            (
                "from funcx_endpoint.endpoint.utils.config import Config\n"
                "from funcx_endpoint.executors import HighThroughputExecutor\n"
                "from parsl.providers import LocalProvider\n"
            ),
            False,
            True,
            True,
            True,
        ],
        [
            (
                "def abc():"
                "    from funcx_endpoint.endpoint.utils.config import Config\n"
                "    from funcx_endpoint.executors import HighThroughputExecutor\n"
                "    from parsl.providers import LocalProvider\n"
                "    return 'hello'\n"
            ),
            False,
            False,
            False,
            False,
        ],
    ],
)
def test_endpoint_update_funcx(mock_get_id, mock_get_conf, fs, cur_config):
    file_content, should_raise, modified, has_bak, do_force = cur_config
    ep_dir = pathlib.Path("some_ep_dir")
    ep_dir.mkdir(parents=True, exist_ok=True)
    with open(ep_dir / "config.py", "w") as f:
        f.write(file_content)
    if has_bak:
        with open(ep_dir / "config.py.bak", "w") as f:
            f.write("old backup data\n")

    try:
        msg = _upgrade_funcx_imports_in_config("some_ep_dir", force=do_force)
        assert not should_raise
        if modified:
            assert "lines were modified" in msg
            assert os.path.exists(ep_dir / "config.py.bak")
        else:
            assert "No funcX import statements" in msg
        with open(ep_dir / "config.py") as f:
            for line in f.readlines():
                assert not line.startswith("from funcx_endpoint.")
    except ClickException as e:
        if should_raise:
            if has_bak and not do_force:
                assert "Rename it or use" in str(e)
        else:
            assert AssertionError(f"Unexpected exception: {e}")
