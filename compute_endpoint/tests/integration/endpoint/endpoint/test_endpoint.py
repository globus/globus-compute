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
from click.testing import CliRunner
from globus_compute_endpoint.cli import _do_stop_endpoint, app
from globus_compute_endpoint.endpoint import endpoint
from globus_compute_endpoint.endpoint.config import UserEndpointConfig
from globus_compute_sdk.sdk.client import _ComputeWebClient

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
    gcc._compute_web_client = _ComputeWebClient(base_url=_SVC_ADDY)
    gcc._compute_web_client.v2.transport.max_retries = 0
    gcc._compute_web_client.v3.transport.max_retries = 0

    yield mocker.patch(f"{_MOCK_BASE}Client", return_value=gcc)


def test_non_configured_endpoint(tmp_path):
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
def test_start_endpoint_display_name(fs, display_name):
    responses.add(  # 404 == we are verifying the POST, not the response
        responses.POST, _SVC_ADDY + "/v3/endpoints", json={}, status=404
    )

    ep = endpoint.Endpoint()
    ep_conf = UserEndpointConfig(engine=mock.Mock())
    ep_dir = pathlib.Path("/some/path/some_endpoint_name")
    ep_dir.mkdir(parents=True, exist_ok=True)
    ep_conf.display_name = display_name

    with pytest.raises(SystemExit) as pyt_exc:
        ep.start_endpoint(ep_dir, None, ep_conf, False, True, reg_info={}, ep_info={})
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
    ep_conf = UserEndpointConfig(engine=mock.Mock())
    ep_dir = pathlib.Path("/some/path/some_endpoint_name")
    ep_dir.mkdir(parents=True, exist_ok=True)
    ep_conf.allowed_functions = [str(uuid.uuid4()), str(uuid.uuid4())]
    ep_conf.authentication_policy = str(uuid.uuid4())
    ep_conf.subscription_id = str(uuid.uuid4())
    ep_conf.public = True

    with pytest.raises(SystemExit) as pyt_exc:
        ep.start_endpoint(ep_dir, None, ep_conf, False, True, reg_info={}, ep_info={})
    assert int(str(pyt_exc.value)) == os.EX_UNAVAILABLE, "Verify exit due to test 404"

    req = pyt_exc.value.__cause__._underlying_response.request
    req_json = json.loads(req.body)

    assert len(req_json["allowed_functions"]) == 2
    assert req_json["allowed_functions"][1] == str(ep_conf.allowed_functions[1])
    assert req_json["authentication_policy"] == str(ep_conf.authentication_policy)
    assert req_json["subscription_uuid"] == str(ep_conf.subscription_id)


def test_stop_remote_endpoint(mocker):
    ep_uuid = "some-uuid"
    ep_dir = pathlib.Path("some_ep_dir") / "abc-endpoint"
    mocker.patch("globus_compute_endpoint.cli.get_config")
    mocker.patch(f"{_MOCK_BASE}Endpoint.get_endpoint_id", return_value=ep_uuid)

    path = f"/v2/endpoints/{ep_uuid}/lock"
    with responses.RequestsMock() as resp:
        lock_resp = resp.post(_SVC_ADDY + path, json={}, status=200)
        _do_stop_endpoint(ep_dir=ep_dir, remote=False)

        assert lock_resp.call_count == 0

        _do_stop_endpoint(ep_dir=ep_dir, remote=True)

        assert lock_resp.call_count == 1


def test_endpoint_setup_execution(mocker, tmp_path, randomstring):
    mocker.patch(f"{_MOCK_BASE}Endpoint.check_pidfile", return_value={"exists": False})

    tmp_file_content = randomstring()
    tmp_file = tmp_path / "random.txt"
    tmp_file.write_text(tmp_file_content)

    exit_code = random.randint(1, 255)  # == avoid rest of endpoint setup
    command = f"cat {tmp_file}\nexit {exit_code}"

    endpoint_dir = None
    endpoint_uuid = None
    endpoint_config = UserEndpointConfig(
        endpoint_setup=command,
        engine=mock.Mock(),
        detach_endpoint=False,
    )
    log_to_console = False
    no_color = True
    reg_info = {}
    ep_info = {}

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
                ep_info,
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
    endpoint_config = UserEndpointConfig(endpoint_teardown=command)

    with mock.patch(f"{_MOCK_BASE}log") as mock_log:
        with pytest.raises(SystemExit) as e:
            endpoint.Endpoint.stop_endpoint(endpoint_dir, endpoint_config)

    assert e.value.code == os.EX_CONFIG

    a, _k = mock_log.error.call_args
    assert "endpoint_teardown failed" in a[0]
    assert f"exit code {exit_code}" in a[0]

    info_txt = "\n".join(a[0] for a, _k in mock_log.info.call_args_list)
    assert tmp_file_content in info_txt
