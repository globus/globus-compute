import uuid
from unittest import mock

import globus_sdk
import pytest
from tests.unit.utils import randomstring

import funcx
from funcx.serialize import FuncXSerializer


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

    # default kwargs: turn off external interactions
    kwargs = {"do_version_check": False, "use_offprocess_checker": False}

    # either pass the env as a param or set it in the environment
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


def test_client_init_accepts_specified_taskgroup(_mock_login):
    tg_uuid = uuid.uuid4()
    fxc = funcx.FuncXClient(
        task_group_id=tg_uuid, do_version_check=False, use_offprocess_checker=False
    )
    assert fxc.session_task_group_id == str(tg_uuid)


@pytest.mark.parametrize(
    "api_data",
    [
        {"status": "Success", "result": True},
        {"status": "success", "result": True},
        # Weird but currently technically possible
        {"status": "Failed", "result": True},
        {"status": "failed", "exception": True},
        {"status": "failed", "reason": randomstring()},
        {"status": "failed", "sentinel": 1},
        {"status": "asdf", "sentinel": 1},
    ],
)
def test_update_task_table_is_robust(_mock_login, api_data):
    payload = randomstring()
    exc = KeyError("asdf")
    task_id = "some_task_id"
    serde = FuncXSerializer()
    data = dict(completion_t=1, **api_data)
    if data.get("result"):
        data["result"] = serde.serialize(payload)
    if data.get("exception"):
        data["exception"] = serde.serialize(exc)

    # test kernel
    fxc = funcx.FuncXClient(do_version_check=False, use_offprocess_checker=False)
    st = fxc._update_task_table(data, task_id)

    # verify results
    if data.get("status", "").lower() in ("success", "failed"):
        assert not st["pending"]
    else:
        assert st["pending"]

    if "result" in data:
        assert st["result"] == payload
        assert "exception" not in st
    elif not st["pending"]:
        assert "result" not in st
        assert "exception" in st

    if "exception" in data:
        assert exc.__class__ is st["exception"].__class__
        assert exc.args == st["exception"].args
    elif "reason" in data:
        assert Exception is st["exception"].__class__
        assert (data["reason"],) == st["exception"].args
    elif not st["pending"] and "result" not in data:
        assert Exception is st["exception"].__class__
        for key in data:
            assert str(key) in str(st["exception"].args)
