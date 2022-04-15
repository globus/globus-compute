import uuid
from unittest import mock

import pytest
from tests.unit.utils import randomstring

import funcx
from funcx.serialize import FuncXSerializer


@pytest.fixture(autouse=True)
def _clear_sdk_env(monkeypatch):
    monkeypatch.delenv("FUNCX_SDK_ENVIRONMENT", raising=False)


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
    kwargs = {
        "login_manager": mock.Mock(),
        "do_version_check": False,
        "use_offprocess_checker": False,
    }
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


def test_client_init_accepts_specified_taskgroup():
    tg_uuid = uuid.uuid4()
    fxc = funcx.FuncXClient(
        task_group_id=tg_uuid,
        do_version_check=False,
        use_offprocess_checker=False,
        login_manager=mock.Mock(),
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
def test_update_task_table_is_robust(api_data):
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
    fxc = funcx.FuncXClient(
        do_version_check=False, use_offprocess_checker=False, login_manager=mock.Mock()
    )
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


def test_pending_tasks_always_fetched():
    should_fetch_01 = str(uuid.uuid4())
    should_fetch_02 = str(uuid.uuid4())
    no_fetch = str(uuid.uuid4())

    fxc = funcx.FuncXClient(
        do_version_check=False, use_offprocess_checker=False, login_manager=mock.Mock()
    )
    fxc.web_client = mock.MagicMock()
    fxc._task_status_table.update(
        {should_fetch_01: {"pending": True}, no_fetch: {"pending": False}}
    )
    task_id_list = [no_fetch, should_fetch_01, should_fetch_02]

    # bulk avenue
    fxc.get_batch_result(task_id_list)

    args, _ = fxc.web_client.get_batch_status.call_args
    assert should_fetch_01 in args[0]
    assert should_fetch_02 in args[0]
    assert no_fetch not in args[0]

    # individual avenue
    for should_fetch, sf in (
        (True, should_fetch_01),
        (True, should_fetch_02),
        (False, no_fetch),
    ):
        fxc.web_client.get_task.reset_mock()
        fxc.get_task(sf)
        assert should_fetch is fxc.web_client.get_task.called
        if should_fetch:
            args, _ = fxc.web_client.get_task.call_args
            assert sf == args[0]
