import logging
import os
import pathlib
import uuid
from unittest.mock import MagicMock, Mock, patch

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
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_sdk.sdk.web_client import WebClient

_MOCK_BASE = "globus_compute_endpoint.endpoint.endpoint."


@pytest.fixture(autouse=True)
def patch_compute_client(mocker):
    return mocker.patch(f"{_MOCK_BASE}Client")


def test_non_configured_endpoint(mocker):
    result = CliRunner().invoke(app, ["start", "newendpoint"])
    assert "newendpoint" in result.stdout
    assert "not configured" in result.stdout


@pytest.mark.parametrize("status_code", [409, 410, 423])
@responses.activate
def test_start_endpoint_blocked(
    mocker, fs, randomstring, patch_compute_client, status_code
):
    # happy-path tested in tests/unit/test_endpoint_unit.py

    svc_addy = "http://api.funcx"
    gcc = globus_compute_sdk.Client(
        funcx_service_address=svc_addy,
        do_version_check=False,
        login_manager=mocker.Mock(),
    )
    gcc.web_client = WebClient(base_url=svc_addy)
    patch_compute_client.return_value = gcc

    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    reason_msg = randomstring()
    responses.add(
        responses.GET,
        svc_addy + "/v2/version",
        json={"api": "1.0.5", "min_ep_version": "1.0.5", "min_sdk_version": "0.0.2a0"},
        status=200,
    )
    responses.add(
        responses.POST,
        svc_addy + "/v2/endpoints",
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


@pytest.mark.parametrize(
    "display_name",
    [
        None,
        "xyz",
        "😎 Great display/.name",
    ],
)
def test_start_endpoint_display_name(mocker, fs, patch_compute_client, display_name):
    svc_addy = "http://api.funcx"
    gcc = globus_compute_sdk.Client(
        funcx_service_address=svc_addy,
        do_version_check=False,
        login_manager=mocker.Mock(),
    )
    gcwc = WebClient(base_url=svc_addy)
    gcwc.post = MagicMock()
    gcc.web_client = gcwc
    patch_compute_client.return_value = gcc

    responses.add(
        responses.GET,
        svc_addy + "/v2/version",
        json={"api": "1.0.5", "min_ep_version": "1.0.5", "min_sdk_version": "0.0.2a0"},
        status=200,
    )
    responses.add(
        responses.POST,
        svc_addy + "/v2/endpoints",
        json={},
        status=200,
    )

    ep = endpoint.Endpoint()
    ep_conf = Config()
    ep_dir = pathlib.Path("/some/path/some_endpoint_name")
    ep_dir.mkdir(parents=True, exist_ok=True)
    ep_conf.display_name = display_name

    with pytest.raises(SystemExit):
        ep.start_endpoint(ep_dir, str(uuid.uuid4()), ep_conf, False, True, reg_info={})

    called_data = gcc.web_client.post.call_args[1]["data"]

    if display_name is not None:
        assert display_name == called_data["display_name"]
    else:
        assert "display_name" not in called_data


def test_start_endpoint_allowlist_passthrough(mocker, fs, patch_compute_client):
    gcc_addy = "http://api.funcx"
    gcc = globus_compute_sdk.Client(
        funcx_service_address=gcc_addy,
        do_version_check=False,
        login_manager=mocker.Mock(),
    )
    gcwc = WebClient(base_url=gcc_addy)
    gcwc.post = MagicMock()
    gcc.web_client = gcwc
    patch_compute_client.return_value = gcc

    responses.add(
        responses.GET,
        gcc_addy + "/v2/version",
        json={"api": "1.0.5", "min_ep_version": "1.0.5", "min_sdk_version": "0.0.2a0"},
        status=200,
    )
    responses.add(
        responses.POST,
        gcc_addy + "/v2/endpoints",
        json={},
        status=200,
    )

    ep = endpoint.Endpoint()
    ep_conf = Config()
    ep_dir = pathlib.Path("/some/path/some_endpoint_name")
    ep_dir.mkdir(parents=True, exist_ok=True)
    ep_conf.allowed_functions = ["a", "b"]

    with pytest.raises(SystemExit):
        ep.start_endpoint(ep_dir, str(uuid.uuid4()), ep_conf, False, True, reg_info={})

    config = gcc.web_client.post.call_args[1]["data"]["metadata"]["config"]
    assert len(config["allowed_functions"]) == 2
    assert config["allowed_functions"][1] == "b"


def test_endpoint_logout(monkeypatch):
    # not forced, and no running endpoints
    logout_true = Mock(return_value=True)
    logout_false = Mock(return_value=False)
    monkeypatch.setattr(
        globus_compute_sdk.sdk.login_manager.LoginManager, "logout", logout_true
    )
    success, msg = _do_logout_endpoints(False, running_endpoints={})
    logout_true.assert_called_once()
    assert success

    logout_true.reset_mock()

    # forced, and no running endpoints
    success, msg = _do_logout_endpoints(True, running_endpoints={})
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


@patch(f"{_MOCK_BASE}Endpoint.get_endpoint_id", return_value="abc-uuid")
@patch(
    "globus_compute_endpoint.cli.get_config_dir",
    return_value=pathlib.Path("some_ep_dir"),
)
@patch("globus_compute_endpoint.cli.get_config")
@patch(f"{_MOCK_BASE}Client.stop_endpoint")
def test_stop_remote_endpoint(
    mock_get_id, mock_get_conf, mock_get_gcc, mock_stop_endpoint
):
    _do_stop_endpoint(name="abc-endpoint", remote=False)
    assert not mock_stop_endpoint.called
    _do_stop_endpoint(name="abc-endpoint", remote=True)
    assert mock_stop_endpoint.called


@patch(f"{_MOCK_BASE}Endpoint.get_endpoint_id", return_value="abc-uuid")
@pytest.mark.parametrize(
    "cur_config",
    [
        ["abc\nbcdcef", False, True, False],
        ["abc\nbcdcef", False, True, True],
        [
            (
                "from funcx_endpoint.endpoint.utils.config import Config\n"
                "from funcx_endpoint.engines import HighThroughputEngine\n"
                "from funcx_endpoint.executors import HighThroughputExecutor\n"
                "from parsl.providers import LocalProvider\n"
                "\n"
                "config = Config(\n"
                "    executors=[\n"
                "        HighThroughputEngine(\n"
                "            provider=LocalProvider(\n"
                "                init_blocks=1,\n"
                "                min_blocks=0,\n"
                "                max_blocks=1,\n"
                "),\n"
            ),
            True,
            False,
            False,
        ],
        [
            (
                "from funcx_endpoint.endpoint.utils.config import Config\n"
                "from funcx_endpoint.engines import HighThroughputEngine\n"
                "from funcx_endpoint.executors import HighThroughputExecutor\n"
                "from parsl.providers import LocalProvider\n"
            ),
            True,
            False,
            True,
        ],
        [
            (
                "from funcx_endpoint.endpoint.utils.config import Config\n"
                "from funcx_endpoint.engines import HighThroughputEngine\n"
                "from funcx_endpoint.executors import HighThroughputExecutor\n"
                "from parsl.providers import LocalProvider\n"
            ),
            True,
            True,
            True,
        ],
        [
            (
                "def abc():"
                "    from funcx_endpoint.endpoint.utils.config import Config\n"
                "    from funcx_endpoint.engines import HighThroughputEngine\n"
                "    from funcx_endpoint.executors import HighThroughputExecutor\n"
                "    from parsl.providers import LocalProvider\n"
                "    return 'hello'\n"
            ),
            False,
            False,
            False,
        ],
    ],
)
def test_endpoint_update_funcx(mock_get_id, mocker, fs, cur_config, randomstring):
    mocker.patch(
        "globus_compute_endpoint.cli.get_config_dir",
        return_value=pathlib.Path(),
    )

    ep_name = randomstring()
    file_content, modified, has_bak, do_force = cur_config
    ep_dir = pathlib.Path(ep_name)
    ep_dir.mkdir(parents=True, exist_ok=True)
    with open(ep_dir / "config.py", "w") as f:
        f.write(file_content)
    if has_bak:
        with open(ep_dir / "config.py.bak", "w") as f:
            f.write("old backup data\n")

    try:
        msg = _upgrade_funcx_imports_in_config(ep_name, force=do_force)
        if modified:
            config_backup = ep_dir / "config.py.bak"
            assert "Applied following diff for endpoint" in msg
            assert ep_name in msg
            assert "renamed to" in msg
            assert str(config_backup) in msg
            assert (ep_dir / "config.py.bak").exists()
        else:
            assert "No funcX import statements" in msg
        with open(ep_dir / "config.py") as f:
            for line in f.readlines():
                assert not line.startswith("from funcx_endpoint.")
    except ClickException as e:
        if has_bak and not do_force:
            assert "Rename it or use" in str(e)
        else:
            raise AssertionError(f"Unexpected exception: ({type(e).__name__}) {e}")


def test_endpoint_setup_execution(mocker, tmp_path, randomstring, caplog):
    mocker.patch(f"{_MOCK_BASE}Endpoint.check_pidfile", return_value={"exists": False})

    tmp_file_content = randomstring()
    tmp_file = tmp_path / "random.txt"
    tmp_file.write_text(tmp_file_content)

    command = f"""\
cat {tmp_file}
exit 1  # exit early to avoid the rest of endpoint setup
    """

    endpoint_dir = None
    endpoint_uuid = None
    endpoint_config = Config(endpoint_setup=command)
    log_to_console = None
    no_color = None
    reg_info = None

    ep = endpoint.Endpoint()
    with caplog.at_level(logging.INFO), pytest.raises(SystemExit) as e:
        ep.start_endpoint(
            endpoint_dir,
            endpoint_uuid,
            endpoint_config,
            log_to_console,
            no_color,
            reg_info,
        )

    assert e.value.code == os.EX_CONFIG
    assert tmp_file_content in caplog.text


def test_endpoint_teardown_execution(mocker, tmp_path, randomstring, caplog):
    mocker.patch(
        f"{_MOCK_BASE}Endpoint.check_pidfile",
        return_value={"exists": True, "active": True},
    )

    tmp_file_content = randomstring()
    tmp_file = tmp_path / "random.txt"
    tmp_file.write_text(tmp_file_content)

    command = f"""\
cat {tmp_file}
exit 1  # exit early to avoid the rest of endpoint teardown
    """

    endpoint_dir = tmp_path
    endpoint_config = Config(endpoint_teardown=command)

    with caplog.at_level(logging.INFO), pytest.raises(SystemExit) as e:
        endpoint.Endpoint.stop_endpoint(
            endpoint_dir,
            endpoint_config,
        )

    assert e.value.code == os.EX_CONFIG
    assert tmp_file_content in caplog.text
