from __future__ import annotations

import collections
import json
import os
import time
import typing as t

import pytest
from globus_compute_sdk import Client
from globus_compute_sdk.sdk.web_client import WebClient
from globus_compute_sdk.serialize.concretes import DillCodeSource
from globus_sdk import AccessTokenAuthorizer, AuthClient, ConfidentialAppAuthClient

# the non-tutorial endpoint will be required, with the following priority order for
# finding the ID:
#
#  1. `--endpoint` opt
#  2. COMPUTE_LOCAL_ENDPOINT_ID (seen here)
#  3. COMPUTE_LOCAL_ENDPOINT_NAME (the name of a dir in `~/.globus_compute/`)
#  4. An endpoint ID found in ~/.globus_compute/default/endpoint.json
#
#  this var starts with the ID env var load
_LOCAL_ENDPOINT_ID = os.getenv("COMPUTE_LOCAL_ENDPOINT_ID")
_LOCAL_FUNCTION_ID = os.getenv("COMPUTE_LOCAL_KNOWN_FUNCTION_ID")

_CONFIGS = {
    "test": {
        "client_args": {"environment": "test"},
        "forwarder_min_version": "0.3.5",
        "api_min_version": "0.3.5",
        "public_hello_fn_uuid": "232966fe-0b32-4434-8cd0-0ca217f8173c",
        "endpoint_uuid": "4b116d3c-1703-4f8f-9f6f-39921e5864df",
    },
    "sandbox": {
        "client_args": {"environment": "sandbox"},
        "forwarder_min_version": "0.3.5",
        "api_min_version": "0.3.5",
        "public_hello_fn_uuid": "3bf4b413-d288-46b7-9808-0cad38db7dec",
        "endpoint_uuid": "4b116d3c-1703-4f8f-9f6f-39921e5864df",
    },
    "preview": {
        "client_args": {"environment": "preview"},
        "forwarder_min_version": "0.3.5",
        "api_min_version": "0.3.5",
        "public_hello_fn_uuid": "566dfa52-4938-4c01-b6f5-eba4cb9898aa",
        "endpoint_uuid": "4b116d3c-1703-4f8f-9f6f-39921e5864df",
    },
    "integration": {
        "client_args": {"environment": "integration"},
        "forwarder_min_version": "0.3.5",
        "api_min_version": "0.3.5",
        "public_hello_fn_uuid": "9815b262-b51c-48d5-8687-712f9616b9f1",
        "endpoint_uuid": "4b116d3c-1703-4f8f-9f6f-39921e5864df",
    },
    "staging": {
        "client_args": {"environment": "staging"},
        "forwarder_min_version": "0.3.5",
        "api_min_version": "0.3.5",
        "public_hello_fn_uuid": "1e840c9d-1ff2-4e45-bf09-248f717df584",
        "endpoint_uuid": "4b116d3c-1703-4f8f-9f6f-39921e5864df",
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
        "client_args": {"environment": "sandbox"},
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
        os.path.expanduser("~"), ".globus_compute", local_endpoint_name, "endpoint.json"
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
        "--local-compute-services",
        action="store_true",
        help="Point smoke tests to local instance of the Compute services",
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

    from globus_compute_sdk.sdk.login_manager import LoginManagerProtocol

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

    # check runtime-checkable protocol
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
    local_compute_services = pytestconfig.getoption("--local-compute-services")

    # env vars to allow use of client creds in GitHub Actions
    api_client_id = os.getenv("FUNCX_SMOKE_CLIENT_ID")
    api_client_secret = os.getenv("FUNCX_SMOKE_CLIENT_SECRET")
    if local_compute_services:
        client_args["local_compute_services"] = local_compute_services

    # Manually add the env here in case the GH actions yaml doesn't pick it up
    sdk_env = client_args.get("environment")
    if sdk_env:
        os.environ["GLOBUS_SDK_ENVIRONMENT"] = sdk_env

    if api_client_id and api_client_secret:
        _add_args_for_client_creds_login(api_client_id, api_client_secret, client_args)

    client_args["code_serialization_strategy"] = DillCodeSource()

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
def timeout_s():
    return 300


class LinearBackoff:
    def __init__(self, timeout_s: int, base_delay_s: int):
        self.timeout_s = timeout_s
        self.delay_s = base_delay_s
        self.start_time: float | None = None

    def backoff(self):
        if self.start_time is None:
            self.start_time = time.monotonic()

        elapsed_s = time.monotonic() - self.start_time
        assert elapsed_s < self.timeout_s, f"Hit {self.timeout_s} second test timeout"

        remaining_s = max(self.timeout_s - elapsed_s, 0)
        self.delay_s = round(min(self.delay_s, remaining_s))
        time.sleep(self.delay_s)

        self.delay_s += 1
        return True


@pytest.fixture(scope="function")
def linear_backoff(timeout_s: int):
    return LinearBackoff(timeout_s, 1).backoff


@pytest.fixture
def submit_function_and_get_result(compute_client: Client, linear_backoff: t.Callable):
    def submit_fn(endpoint_id, func=None, func_args=None, func_kwargs=None):
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

        while linear_backoff():
            response = compute_client.get_task(task_id)
            if response.get("pending") is False:
                result = response.get("result")
                break

        return FuncResult(func_id, task_id, result, response)

    return submit_fn
