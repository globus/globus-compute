import uuid
from unittest import mock

import pytest

import funcx
from funcx.errors import FuncxTaskExecutionFailed
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
        login_manager=mock.Mock(),
    )
    assert fxc.session_task_group_id == str(tg_uuid)


@pytest.mark.parametrize(
    "api_data",
    [
        {"status": "Success"},  # success without result|exception
        {"status": "FAILED"},  # failure without result|exception
        "abc123",  # string, but not valid JSON
    ],
)
def test_update_task_table_on_invalid_data(api_data):
    fxc = funcx.FuncXClient(do_version_check=False, login_manager=mock.Mock())

    with pytest.raises(ValueError):
        fxc._update_task_table(api_data, "task-id-foo")


def test_update_task_table_on_exception():
    api_data = {"status": "success", "exception": "foo-bar-baz", "completion_t": "1.1"}
    fxc = funcx.FuncXClient(do_version_check=False, login_manager=mock.Mock())

    with pytest.raises(FuncxTaskExecutionFailed) as excinfo:
        fxc._update_task_table(api_data, "task-id-foo")
    assert "foo-bar-baz" in str(excinfo.value)


def test_update_task_table_simple_object(randomstring):
    serde = FuncXSerializer()
    fxc = funcx.FuncXClient(do_version_check=False, login_manager=mock.Mock())
    task_id = "some_task_id"

    payload = randomstring()
    data = {"status": "success", "completion_t": "1.1"}
    data["result"] = serde.serialize(payload)

    st = fxc._update_task_table(data, task_id)
    assert not st["pending"]
    assert st["result"] == payload
    assert "exception" not in st


def test_pending_tasks_always_fetched():
    should_fetch_01 = str(uuid.uuid4())
    should_fetch_02 = str(uuid.uuid4())
    no_fetch = str(uuid.uuid4())

    fxc = funcx.FuncXClient(do_version_check=False, login_manager=mock.Mock())
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


@pytest.mark.parametrize("create_ws_queue", [True, False, None])
def test_batch_created_websocket_queue(create_ws_queue):
    eid = str(uuid.uuid4())
    fid = str(uuid.uuid4())

    fxc = funcx.FuncXClient(do_version_check=False, login_manager=mock.Mock())
    fxc.web_client = mock.MagicMock()
    if create_ws_queue is None:
        batch = fxc.create_batch()
    else:
        batch = fxc.create_batch(create_websocket_queue=create_ws_queue)

    batch.add(fid, eid, (1,))
    batch.add(fid, eid, (2,))

    fxc.batch_run(batch)

    assert fxc.web_client.submit.called
    submit_data = fxc.web_client.submit.call_args[0][0]
    assert "create_websocket_queue" in submit_data
    if create_ws_queue:
        assert submit_data["create_websocket_queue"] is True
    else:
        assert submit_data["create_websocket_queue"] is False


@pytest.mark.parametrize("asynchronous", [True, False, None])
def test_single_run_websocket_queue_depend_async(asynchronous):

    if asynchronous is None:
        fxc = funcx.FuncXClient(do_version_check=False, login_manager=mock.Mock())
    else:
        fxc = funcx.FuncXClient(
            asynchronous=asynchronous, do_version_check=False, login_manager=mock.Mock()
        )

    fxc.web_client = mock.MagicMock()

    fake_results = {
        "results": [
            {
                "task_uuid": str(uuid.uuid4()),
                "http_status_code": 200,
            }
        ],
        "task_group_id": str(uuid.uuid4()),
    }
    fxc.web_client.submit = mock.MagicMock(return_value=fake_results)
    fxc.run(endpoint_id=str(uuid.uuid4()), function_id=str(uuid.uuid4()))

    assert fxc.web_client.submit.called
    submit_data = fxc.web_client.submit.call_args[0][0]
    assert "create_websocket_queue" in submit_data
    if asynchronous:
        assert submit_data["create_websocket_queue"] is True
    else:
        assert submit_data["create_websocket_queue"] is False
