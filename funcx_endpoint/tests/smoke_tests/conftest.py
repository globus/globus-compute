import json
import os

import pytest
from globus_sdk import AccessTokenAuthorizer, ConfidentialAppAuthClient

from funcx import FuncXClient
from funcx.sdk.executor import FuncXExecutor

# the non-tutorial endpoint will be required, with the following priority order for
# finding the ID:
#
#  1. `--endpoint` opt
#  2. FUNX_LOCAL_ENDPOINT_ID (seen here)
#  3. FUNX_LOCAL_ENDPOINT_NAME (the name of a dir in `~/.funcx/`)
#  4. An endpoint ID found in ~/.funcx/default/endpoint.json
#
#  this var starts with the ID env var load
_LOCAL_ENDPOINT_ID = os.getenv("FUNCX_LOCAL_ENDPOINT_ID")

_CONFIGS = {
    "dev": {
        "client_args": {
            "funcx_service_address": "https://api.dev.funcx.org/v2",
            "results_ws_uri": "wss://api.dev.funcx.org/ws/v2/",
        },
        # assert versions are as expected on prod
        "forwarder_version": "0.3.5",
        "api_version": "0.3.5",
        # This fn is public and searchable
        "public_hello_fn_uuid": "f84351f9-6f82-45d8-8eca-80d8f73645be",
        "endpoint_uuid": _LOCAL_ENDPOINT_ID,
    },
    "prod": {
        # By default tests are against production, which means we do not need to pass
        # any arguments to the client object (default will point at prod stack)
        "client_args": {},
        # assert versions are as expected on prod
        "forwarder_version": "0.3.5",
        "api_version": "0.3.5",
        # This fn is public and searchable
        "public_hello_fn_uuid": "b0a5d1a0-2b22-4381-b899-ba73321e41e0",
        # For production tests, the target endpoint should be the tutorial_endpoint
        "endpoint_uuid": "4b116d3c-1703-4f8f-9f6f-39921e5864df",
    },
    "local": {
        # localhost; typical defaults for a helm deploy
        "client_args": {
            "funcx_service_address": "http://localhost:5000/v2",
            "results_ws_uri": "ws://localhost:6000/ws/v2/",
        },
        "endpoint_uuid": _LOCAL_ENDPOINT_ID,
    },
}


def _get_local_endpoint_id():
    # get the ID of a local endpoint, by name
    # this is only called if
    #  - there is no endpoint in the config (e.g. config via env var)
    #  - `--endpoint` is not passed
    local_endpoint_name = os.getenv("FUNCX_LOCAL_ENDPOINT_NAME", "default")
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
    """Add funcx-specific command-line options to pytest."""
    parser.addoption(
        "--funcx-config", default="prod", help="Name of testing config to use"
    )
    parser.addoption(
        "--endpoint", metavar="endpoint", help="Specify an active endpoint UUID"
    )
    parser.addoption(
        "--service-address",
        metavar="service-address",
        help="Specify a funcX service address",
    )
    parser.addoption(
        "--ws-uri", metavar="ws-uri", help="WebSocket URI to get task results"
    )
    parser.addoption(
        "--api-client-id",
        metavar="api-client-id",
        default=None,
        help="The API Client ID. Used for github actions testing",
    )
    parser.addoption(
        "--api-client-secret",
        metavar="api-client-secret",
        default=None,
        help="The API Client Secret. Used for github actions testing",
    )


@pytest.fixture(scope="session")
def funcx_test_config_name(pytestconfig):
    return pytestconfig.getoption("--funcx-config")


@pytest.fixture(scope="session")
def funcx_test_config(pytestconfig, funcx_test_config_name):
    # start with basic config load
    config = _CONFIGS[funcx_test_config_name]

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
    api_client_id = pytestconfig.getoption("--api-client-id")
    api_client_secret = pytestconfig.getoption("--api-client-secret")
    if ws_uri:
        client_args["results_ws_uri"] = ws_uri
    if api_uri:
        client_args["funcx_service_address"] = api_uri

    if api_client_id and api_client_secret:
        client = ConfidentialAppAuthClient(api_client_id, api_client_secret)
        scopes = [
            "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
            "urn:globus:auth:scope:search.api.globus.org:all",
            "openid",
        ]

        token_response = client.oauth2_client_credentials_tokens(
            requested_scopes=scopes
        )
        fx_token = token_response.by_resource_server["funcx_service"]["access_token"]
        search_token = token_response.by_resource_server["search.api.globus.org"][
            "access_token"
        ]
        openid_token = token_response.by_resource_server["auth.globus.org"][
            "access_token"
        ]

        fx_auth = AccessTokenAuthorizer(fx_token)
        search_auth = AccessTokenAuthorizer(search_token)
        openid_auth = AccessTokenAuthorizer(openid_token)

        client_args["fx_authorizer"] = fx_auth
        client_args["search_authorizer"] = search_auth
        client_args["openid_authorizer"] = openid_auth

    return config


@pytest.fixture(scope="session")
def fxc(funcx_test_config):
    client_args = funcx_test_config["client_args"]
    fxc = FuncXClient(**client_args)
    return fxc


@pytest.fixture(scope="session")
def async_fxc(funcx_test_config):
    client_args = funcx_test_config["client_args"]
    fxc = FuncXClient(**client_args, asynchronous=True)
    return fxc


@pytest.fixture(scope="session")
def fx(fxc):
    fx = FuncXExecutor(fxc)
    return fx


@pytest.fixture
def endpoint(funcx_test_config):
    return funcx_test_config["endpoint_uuid"]


@pytest.fixture
def tutorial_funcion_id(funcx_test_config):
    funcid = funcx_test_config.get("public_hello_fn_uuid")
    if not funcid:
        pytest.skip("test requires a pre-defined public hello function")
    return funcid
