from unittest import mock

import globus_sdk
import pytest

import funcx


@pytest.fixture(autouse=True)
def _clear_sdk_env(monkeypatch):
    monkeypatch.delenv("FUNCX_SDK_ENVIRONMENT", raising=False)


@pytest.fixture(autouse=True)
def _mock_login(monkeypatch):
    mock_fair_research_client_object = mock.Mock()
    monkeypatch.setattr(
        "funcx.sdk.client.NativeClient",
        mock.Mock(return_value=mock_fair_research_client_object),
    )
    mock_fair_research_client_object.get_authorizers_by_scope.return_value = {
        "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all": (
            globus_sdk.NullAuthorizer()
        ),
        "urn:globus:auth:scope:search.api.globus.org:all": globus_sdk.NullAuthorizer(),
        "openid": globus_sdk.NullAuthorizer(),
    }

    mock_auth_client = mock.Mock()
    monkeypatch.setattr(
        "funcx.sdk.client.AuthClient", mock.Mock(return_value=mock_auth_client)
    )
    mock_auth_client.oauth2_userinfo.return_value = {"sub": "foo"}

    monkeypatch.setattr("funcx.sdk.client.SearchHelper", mock.Mock())
    monkeypatch.setattr("funcx.sdk.client.FuncXClient.version_check", mock.Mock())


@pytest.mark.parametrize("env", [None, "dev", "production"])
@pytest.mark.parametrize("usage_method", ["env_var", "param"])
@pytest.mark.parametrize("explicit_params", ["neither", "web", "ws", "both"])
def test_client_init_sets_addresses_by_env(
    monkeypatch, env, usage_method, explicit_params
):
    if env in (None, "production"):
        web_uri = "https://api2.funcx.org/v2"
        ws_uri = "wss://api2.funcx.org/ws/v2/"
    elif env in ("dev",):
        web_uri = "https://api.dev.funcx.org/v2"
        ws_uri = "wss://api.dev.funcx.org/ws/v2/"
    else:
        raise NotImplementedError

    # either pass the env as a param or set it in the environment
    kwargs = {}
    if usage_method == "param":
        kwargs["environment"] = env
    elif usage_method == "env_var":
        if env is not None:
            monkeypatch.setenv("FUNCX_SDK_ENVIRONMENT", env)
    else:
        raise NotImplementedError

    # create the client, either with just the input env or with explicit parameters
    # for explicit params, alter the expected URI(s)
    if explicit_params == "neither":
        client = funcx.FuncXClient(**kwargs)
    elif explicit_params == "web":
        web_uri = "http://localhost:5000"
        client = funcx.FuncXClient(funcx_service_address=web_uri, **kwargs)
    elif explicit_params == "ws":
        ws_uri = "ws://localhost:8081"
        client = funcx.FuncXClient(results_ws_uri=ws_uri, **kwargs)
    elif explicit_params == "both":
        web_uri = "http://localhost:5000"
        ws_uri = "ws://localhost:8081"
        client = funcx.FuncXClient(
            funcx_service_address=web_uri, results_ws_uri=ws_uri, **kwargs
        )
    else:
        raise NotImplementedError

    # finally, confirm that the addresses were set correctly
    assert client.funcx_service_address == web_uri
    assert client.results_ws_uri == ws_uri
