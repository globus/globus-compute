import json
import os
import pathlib
import random
import uuid
from unittest import mock

import globus_compute_sdk.sdk.client
import globus_compute_sdk.sdk.login_manager
import pytest
import responses
from click import ClickException
from click.testing import CliRunner
from globus_compute_endpoint.cli import (
    _do_stop_endpoint,
    _upgrade_funcx_imports_in_config,
    app,
)
from globus_compute_endpoint.endpoint import endpoint
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_sdk.sdk.web_client import WebClient

_MOCK_BASE = "globus_compute_endpoint.endpoint.endpoint."
_SVC_ADDY = "http://api.funcx.fqdn"  # something clearly not correct


@pytest.fixture(autouse=True)
def patch_compute_client(mocker):
    responses.add(
        responses.GET,
        _SVC_ADDY + "/v2/version",
        json={"api": "1.2.0", "min_ep_version": "2.0.0", "min_sdk_version": "1.0.0a6"},
        status=200,
    )

    gcc = globus_compute_sdk.Client(
        do_version_check=False,
        login_manager=mock.Mock(),
    )
    gcc.web_service_address = _SVC_ADDY
    gcc.web_client = WebClient(base_url=_SVC_ADDY)

    yield mocker.patch(f"{_MOCK_BASE}Client", return_value=gcc)


def test_non_configured_endpoint(mocker, tmp_path):
    env = {"GLOBUS_COMPUTE_USER_DIR": str(tmp_path)}
    with mock.patch.dict(os.environ, env):
        result = CliRunner().invoke(app, ["start", "newendpoint"])
        assert "newendpoint" in result.stdout
        assert "no endpoint configuration" in result.stdout


@pytest.mark.parametrize(
    "display_name",
    [
        None,
        "xyz",
        "😎 Great display/.name",
    ],
)
def test_start_endpoint_display_name(mocker, fs, display_name):
    responses.add(  # 404 == we are verifying the POST, not the response
        responses.POST, _SVC_ADDY + "/v3/endpoints", json={}, status=404
    )

    ep = endpoint.Endpoint()
    ep_conf = Config()
    ep_dir = pathlib.Path("/some/path/some_endpoint_name")
    ep_dir.mkdir(parents=True, exist_ok=True)
    ep_conf.display_name = display_name

    with pytest.raises(SystemExit) as pyt_exc:
        ep.start_endpoint(ep_dir, None, ep_conf, False, True, reg_info={})
    assert int(str(pyt_exc.value)) == os.EX_UNAVAILABLE, "Verify exit due to test 404"

    req = pyt_exc.value.__cause__._underlying_response.request
    req_json = json.loads(req.body)
    if display_name is not None:
        assert display_name == req_json["display_name"]
    else:
        assert "display_name" not in req_json


def test_start_endpoint_data_passthrough(fs):
    responses.add(  # 404 == we are verifying the POST, not the response
        responses.POST, _SVC_ADDY + "/v3/endpoints", json={}, status=404
    )

    ep = endpoint.Endpoint()
    ep_conf = Config()
    ep_dir = pathlib.Path("/some/path/some_endpoint_name")
    ep_dir.mkdir(parents=True, exist_ok=True)
    ep_conf.allowed_functions = [str(uuid.uuid4()), str(uuid.uuid4())]
    ep_conf.authentication_policy = str(uuid.uuid4())
    ep_conf.subscription_id = str(uuid.uuid4())
    ep_conf.public = True

    with pytest.raises(SystemExit) as pyt_exc:
        ep.start_endpoint(ep_dir, None, ep_conf, False, True, reg_info={})
    assert int(str(pyt_exc.value)) == os.EX_UNAVAILABLE, "Verify exit due to test 404"

    req = pyt_exc.value.__cause__._underlying_response.request
    req_json = json.loads(req.body)

    assert len(req_json["allowed_functions"]) == 2
    assert req_json["allowed_functions"][1] == ep_conf.allowed_functions[1]
    assert req_json["authentication_policy"] == ep_conf.authentication_policy
    assert req_json["subscription_uuid"] == ep_conf.subscription_id
    assert req_json["public"] is ep_conf.public


@mock.patch(f"{_MOCK_BASE}Endpoint.get_endpoint_id", return_value="abc-uuid")
@mock.patch("globus_compute_endpoint.cli.get_config")
@mock.patch(f"{_MOCK_BASE}Client.stop_endpoint")
def test_stop_remote_endpoint(mock_get_id, mock_get_gcc, mock_stop_endpoint):
    ep_dir = pathlib.Path("some_ep_dir") / "abc-endpoint"

    _do_stop_endpoint(ep_dir=ep_dir, remote=False)
    assert not mock_stop_endpoint.called

    path = f"/v2/endpoints/{mock_stop_endpoint.return_value}/lock"
    responses.add(responses.POST, _SVC_ADDY + path, json={}, status=200)

    _do_stop_endpoint(ep_dir=ep_dir, remote=True)
    assert mock_stop_endpoint.called


@mock.patch(f"{_MOCK_BASE}Endpoint.get_endpoint_id", return_value="abc-uuid")
@pytest.mark.parametrize(
    "cur_config",
    [
        ["abc\nbcdcef", False, True, False],
        ["abc\nbcdcef", False, True, True],
        [
            (
                "from funcx_endpoint.endpoint.utils.config import Config\n"
                "from funcx_endpoint.engines import GlobusComputeEngine\n"
                "from funcx_endpoint.executors import HighThroughputExecutor\n"
                "from parsl.providers import LocalProvider\n"
                "\n"
                "config = Config(\n"
                "    executors=[\n"
                "        GlobusComputeEngine(\n"
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
                "from funcx_endpoint.engines import GlobusComputeEngine\n"
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
                "from funcx_endpoint.engines import GlobusComputeEngine\n"
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
                "    from funcx_endpoint.engines import GlobusComputeEngine\n"
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
        msg = _upgrade_funcx_imports_in_config(ep_dir, force=do_force)
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


def test_endpoint_setup_execution(mocker, tmp_path, randomstring):
    mocker.patch(f"{_MOCK_BASE}Endpoint.check_pidfile", return_value={"exists": False})

    tmp_file_content = randomstring()
    tmp_file = tmp_path / "random.txt"
    tmp_file.write_text(tmp_file_content)

    exit_code = random.randint(1, 255)  # == avoid rest of endpoint setup
    command = f"cat {tmp_file}\nexit {exit_code}"

    endpoint_dir = None
    endpoint_uuid = None
    endpoint_config = Config(endpoint_setup=command, detach_endpoint=False)
    log_to_console = False
    no_color = True
    reg_info = {}

    ep = endpoint.Endpoint()
    with mock.patch(f"{_MOCK_BASE}log") as mock_log:
        with pytest.raises(SystemExit) as e:
            ep.start_endpoint(
                endpoint_dir,
                endpoint_uuid,
                endpoint_config,
                log_to_console,
                no_color,
                reg_info,
            )

    assert e.value.code == os.EX_CONFIG

    a, _k = mock_log.error.call_args
    assert "endpoint_setup failed" in a[0]
    assert f"exit code {exit_code}" in a[0]

    info_txt = "\n".join(a[0] for a, _k in mock_log.info.call_args_list)
    assert tmp_file_content in info_txt


def test_endpoint_teardown_execution(mocker, tmp_path, randomstring):
    mocker.patch(
        f"{_MOCK_BASE}Endpoint.check_pidfile",
        return_value={"exists": True, "active": True},
    )

    tmp_file_content = randomstring()
    tmp_file = tmp_path / "random.txt"
    tmp_file.write_text(tmp_file_content)

    exit_code = random.randint(1, 255)  # == avoid rest of endpoint setup
    command = f"cat {tmp_file}\nexit {exit_code}"

    endpoint_dir = tmp_path
    endpoint_config = Config(endpoint_teardown=command)

    with mock.patch(f"{_MOCK_BASE}log") as mock_log:
        with pytest.raises(SystemExit) as e:
            endpoint.Endpoint.stop_endpoint(endpoint_dir, endpoint_config)

    assert e.value.code == os.EX_CONFIG

    a, _k = mock_log.error.call_args
    assert "endpoint_teardown failed" in a[0]
    assert f"exit code {exit_code}" in a[0]

    info_txt = "\n".join(a[0] for a, _k in mock_log.info.call_args_list)
    assert tmp_file_content in info_txt
