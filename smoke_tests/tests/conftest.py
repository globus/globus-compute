from __future__ import annotations

import collections
import json
import os
import sys
import time

import pytest
from globus_compute_sdk import Client
from globus_compute_sdk.sdk.web_client import WebClient
from globus_sdk import AccessTokenAuthorizer, AuthClient, ConfidentialAppAuthClient

# the non-tutorial endpoint will be required, with the following priority order for
# finding the ID:
#
#  1. `--endpoint` opt
#  2. COMPUTE_LOCAL_ENDPOINT_ID (seen here)
#  3. COMPUTE_LOCAL_ENDPOINT_NAME (the name of a dir in `~/.funcx/`)
#  4. An endpoint ID found in ~/.funcx/default/endpoint.json
#
#  this var starts with the ID env var load
_LOCAL_ENDPOINT_ID = os.getenv("COMPUTE_LOCAL_ENDPOINT_ID")
_LOCAL_FUNCTION_ID = os.getenv("COMPUTE_LOCAL_KNOWN_FUNCTION_ID")

_CONFIGS = {
    "dev": {
        "client_args": {"environment": "dev"},
        # assert versions are as expected on dev
        "forwarder_min_version": "0.3.5",
        "api_min_version": "0.3.5",
        # This fn is public
        "public_hello_fn_uuid": "f84351f9-6f82-45d8-8eca-80d8f73645be",
        "endpoint_uuid": "2238617a-8756-4030-a8ab-44ffb1446092",
    },
    "prod": {
        # By default tests are against production, which means we do not need to pass
        # any arguments to the client object (default will point at prod stack)
        "client_args": {},
        # assert versions are as expected on prod
        "forwarder_min_version": "0.3.5",
        "api_min_version": "0.3.5",
        # This fn is public
        "public_hello_fn_uuid": "b0a5d1a0-2b22-4381-b899-ba73321e41e0",
        # For production tests, the target endpoint should be the tutorial_endpoint
        "endpoint_uuid": "4b116d3c-1703-4f8f-9f6f-39921e5864df",
    },
    "local": {
        # localhost; typical defaults for a helm deploy
        "client_args": {"environment": "local"},
        "public_hello_fn_uuid": _LOCAL_FUNCTION_ID,
        "endpoint_uuid": _LOCAL_ENDPOINT_ID,
    },
}


def _get_local_endpoint_id():
    # get the ID of a local endpoint, by name
    # this is only called if
    #  - there is no endpoint in the config (e.g. config via env var)
    #  - `--endpoint` is not passed
    local_endpoint_name = os.getenv("COMPUTE_LOCAL_ENDPOINT_NAME", "default")
    data_path = os.path.join(
        os.path.expanduser("~"), ".funcx", local_endpoint_name, "endpoint.json"
    )

    try:
        with open(data_path) as fp:
            data = json.load(fp)
    except Exception:
        return None
    else:
        return data["endpoint_id"]


def pytest_addoption(parser):
    """Add command-line options to pytest."""
    parser.addoption(
        "--compute-config", default="prod", help="Name of testing config to use"
    )
    parser.addoption(
        "--endpoint", metavar="endpoint", help="Specify an active endpoint UUID"
    )
    parser.addoption(
        "--service-address",
        metavar="service-address",
        help="Specify a Globus Compute service address",
    )
    parser.addoption(
        "--ws-uri", metavar="ws-uri", help="WebSocket URI to get task results"
    )


@pytest.fixture(scope="session")
def compute_test_config_name(pytestconfig):
    return pytestconfig.getoption("--compute-config")


def _add_args_for_client_creds_login(api_client_id, api_client_secret, client_args):
    auth_client = ConfidentialAppAuthClient(api_client_id, api_client_secret)
    scopes = [
        "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
        AuthClient.scopes.openid,
    ]
    tokens = auth_client.oauth2_client_credentials_tokens(
        requested_scopes=scopes
    ).by_resource_server

    funcx_token = tokens["funcx_service"]["access_token"]
    auth_token = tokens[AuthClient.resource_server]["access_token"]

    funcx_authorizer = AccessTokenAuthorizer(funcx_token)
    auth_authorizer = AccessTokenAuthorizer(auth_token)

    try:
        from globus_compute_sdk.sdk.login_manager import LoginManagerProtocol
    except ImportError:
        client_args["fx_authorizer"] = funcx_authorizer
        client_args["openid_authorizer"] = auth_authorizer
    else:

        class TestsuiteLoginManager:
            def ensure_logged_in(self) -> None:
                pass

            def logout(self) -> None:
                pass

            def get_auth_client(self) -> AuthClient:
                return AuthClient(authorizer=auth_authorizer)

            def get_web_client(self, *, base_url: str | None = None) -> WebClient:
                return WebClient(base_url=base_url, authorizer=funcx_authorizer)

        login_manager = TestsuiteLoginManager()

        # check runtime-checkable protocol on python versions which support it
        if sys.version_info >= (3, 8):
            assert isinstance(login_manager, LoginManagerProtocol)

        client_args["login_manager"] = login_manager


@pytest.fixture(scope="session")
def compute_test_config(pytestconfig, compute_test_config_name):
    # start with basic config load
    config = _CONFIGS[compute_test_config_name]

    # if `--endpoint` was passed or `endpoint_uuid` is present in config,
    # handle those cases
    endpoint = pytestconfig.getoption("--endpoint")
    if endpoint:
        config["endpoint_uuid"] = endpoint
    elif config["endpoint_uuid"] is None:
        config["endpoint_uuid"] = _get_local_endpoint_id()
    if not config["endpoint_uuid"]:
        # If there's no endpoint_uuid available, the smoke tests won't work
        raise Exception("No target endpoint_uuid available to test against")

    # set URIs if passed
    client_args = config["client_args"]
    ws_uri = pytestconfig.getoption("--ws-uri")
    api_uri = pytestconfig.getoption("--service-address")

    # env vars to allow use of client creds in GitHub Actions
    api_client_id = os.getenv("FUNCX_SMOKE_CLIENT_ID")
    api_client_secret = os.getenv("FUNCX_SMOKE_CLIENT_SECRET")
    if ws_uri:
        client_args["results_ws_uri"] = ws_uri
    if api_uri:
        client_args["funcx_service_address"] = api_uri

    if api_client_id and api_client_secret:
        _add_args_for_client_creds_login(api_client_id, api_client_secret, client_args)

    return config


@pytest.fixture(scope="session")
def compute_client(compute_test_config):
    client_args = compute_test_config["client_args"]
    gcc = Client(**client_args)
    return gcc


@pytest.fixture
def endpoint(compute_test_config):
    return compute_test_config["endpoint_uuid"]


@pytest.fixture
def tutorial_function_id(compute_test_config):
    funcid = compute_test_config.get("public_hello_fn_uuid")
    if not funcid:
        pytest.skip("test requires a pre-defined public hello function")
    return funcid


FuncResult = collections.namedtuple(
    "FuncResult", ["func_id", "task_id", "result", "response"]
)


@pytest.fixture
def submit_function_and_get_result(compute_client):
    def submit_fn(
        endpoint_id, func=None, func_args=None, func_kwargs=None, initial_sleep=0
    ):
        if callable(func):
            func_id = compute_client.register_function(func)
        else:
            func_id = func

        if func_args is None:
            func_args = ()
        if func_kwargs is None:
            func_kwargs = {}

        task_id = compute_client.run(
            *func_args, endpoint_id=endpoint_id, function_id=func_id, **func_kwargs
        )

        if initial_sleep:
            time.sleep(initial_sleep)

        result = None
        response = None
        for attempt in range(10):
            response = compute_client.get_task(task_id)
            if response.get("pending") is False:
                result = response.get("result")
            else:
                time.sleep(attempt)

        return FuncResult(func_id, task_id, result, response)

    return submit_fn
