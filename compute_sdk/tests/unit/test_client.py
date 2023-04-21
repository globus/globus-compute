import uuid
from unittest import mock

import globus_compute_sdk as gc
import pytest
from globus_compute_sdk import ContainerSpec
from globus_compute_sdk.errors import TaskExecutionFailed
from globus_compute_sdk.serialize import ComputeSerializer


@pytest.fixture(autouse=True)
def _clear_sdk_env(monkeypatch):
    monkeypatch.delenv("FUNCX_SDK_ENVIRONMENT", raising=False)


@pytest.mark.parametrize("env", [None, "local", "dev", "production"])
@pytest.mark.parametrize("usage_method", ["env_var", "param"])
@pytest.mark.parametrize("explicit_params", [None, "web"])
def test_client_init_sets_addresses_by_env(
    monkeypatch, env, usage_method, explicit_params, randomstring
):
    if env in (None, "production"):
        web_uri = "https://compute.api.globus.org/v2"
    elif env == "dev":
        web_uri = "https://api.dev.funcx.org/v2"
    elif env == "local":
        web_uri = "http://localhost:5000/v2"
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
    if not explicit_params:
        client = gc.Client(**kwargs)
    elif explicit_params == "web":
        web_uri = f"http://{randomstring()}.fqdn:1234/{randomstring()}"
        client = gc.Client(funcx_service_address=web_uri, **kwargs)
    else:
        raise NotImplementedError

    # finally, confirm that the address was set correctly
    assert client.funcx_service_address == web_uri


def test_client_init_accepts_specified_taskgroup():
    tg_uuid = uuid.uuid4()
    gcc = gc.Client(
        task_group_id=tg_uuid,
        do_version_check=False,
        login_manager=mock.Mock(),
    )
    assert gcc.session_task_group_id == str(tg_uuid)


@pytest.mark.parametrize(
    "api_data",
    [
        {"status": "Success"},  # success without result|exception
        {"status": "FAILED"},  # failure without result|exception
        "abc123",  # string, but not valid JSON
    ],
)
def test_update_task_table_on_invalid_data(api_data):
    gcc = gc.Client(do_version_check=False, login_manager=mock.Mock())

    with pytest.raises(ValueError):
        gcc._update_task_table(api_data, "task-id-foo")


def test_update_task_table_on_exception():
    api_data = {"status": "success", "exception": "foo-bar-baz", "completion_t": "1.1"}
    gcc = gc.Client(do_version_check=False, login_manager=mock.Mock())

    with pytest.raises(TaskExecutionFailed) as excinfo:
        gcc._update_task_table(api_data, "task-id-foo")
    assert "foo-bar-baz" in str(excinfo.value)


def test_update_task_table_simple_object(randomstring):
    serde = ComputeSerializer()
    gcc = gc.Client(do_version_check=False, login_manager=mock.Mock())
    task_id = "some_task_id"

    payload = randomstring()
    data = {"status": "success", "completion_t": "1.1"}
    data["result"] = serde.serialize(payload)

    st = gcc._update_task_table(data, task_id)
    assert not st["pending"]
    assert st["result"] == payload
    assert "exception" not in st


def test_pending_tasks_always_fetched():
    should_fetch_01 = str(uuid.uuid4())
    should_fetch_02 = str(uuid.uuid4())
    no_fetch = str(uuid.uuid4())

    gcc = gc.Client(do_version_check=False, login_manager=mock.Mock())
    gcc.web_client = mock.MagicMock()
    gcc._task_status_table.update(
        {should_fetch_01: {"pending": True}, no_fetch: {"pending": False}}
    )
    task_id_list = [no_fetch, should_fetch_01, should_fetch_02]

    # bulk avenue
    gcc.get_batch_result(task_id_list)

    args, _ = gcc.web_client.get_batch_status.call_args
    assert should_fetch_01 in args[0]
    assert should_fetch_02 in args[0]
    assert no_fetch not in args[0]

    # individual avenue
    for should_fetch, sf in (
        (True, should_fetch_01),
        (True, should_fetch_02),
        (False, no_fetch),
    ):
        gcc.web_client.get_task.reset_mock()
        gcc.get_task(sf)
        assert should_fetch is gcc.web_client.get_task.called
        if should_fetch:
            args, _ = gcc.web_client.get_task.call_args
            assert sf == args[0]


@pytest.mark.parametrize("create_ws_queue", [True, False, None])
def test_batch_created_websocket_queue(create_ws_queue):
    eid = str(uuid.uuid4())
    fid = str(uuid.uuid4())

    gcc = gc.Client(do_version_check=False, login_manager=mock.Mock())
    gcc.web_client = mock.MagicMock()
    if create_ws_queue is None:
        batch = gcc.create_batch()
    else:
        batch = gcc.create_batch(create_websocket_queue=create_ws_queue)

    batch.add(fid, eid, (1,))
    batch.add(fid, eid, (2,))

    gcc.batch_run(batch)

    assert gcc.web_client.submit.called
    submit_data = gcc.web_client.submit.call_args[0][0]
    assert "create_websocket_queue" in submit_data
    if create_ws_queue:
        assert submit_data["create_websocket_queue"] is True
    else:
        assert submit_data["create_websocket_queue"] is False


def test_batch_error():
    gcc = gc.Client(do_version_check=False, login_manager=mock.Mock())
    gcc.web_client = mock.MagicMock()

    error_reason = "reason 1 2 3"
    error_results = {
        "response": "batch",
        "results": [
            {
                "http_status_code": 200,
                "status": "Success",
                "task_uuid": "abc",
            },
            {
                "http_status_code": 400,
                "status": "Failed",
                "task_uuid": "def",
                "reason": error_reason,
            },
        ],
        "task_group_id": "tg_id",
    }
    gcc.web_client.submit = mock.MagicMock(return_value=error_results)

    batch = gcc.create_batch()
    batch.add("fid1", "eid1", "arg1")
    batch.add("fid2", "eid2", "arg2")
    with pytest.raises(TaskExecutionFailed) as excinfo:
        gcc.batch_run(batch)

    assert error_reason in str(excinfo)


def test_batch_no_reason():
    gcc = gc.Client(do_version_check=False, login_manager=mock.Mock())
    gcc.web_client = mock.MagicMock()

    error_results = {
        "response": "batch",
        "results": [
            {
                "http_status_code": 500,
                "status": "Failed",
                "task_uuid": "def",
            },
        ],
        "task_group_id": "tg_id",
    }
    gcc.web_client.submit = mock.MagicMock(return_value=error_results)

    with pytest.raises(TaskExecutionFailed) as excinfo:
        gcc.run(endpoint_id="fid", function_id="fid")

    assert "Unknown execution failure" in str(excinfo)


@pytest.mark.parametrize("asynchronous", [True, False, None])
def test_single_run_websocket_queue_depend_async(asynchronous):
    if asynchronous is None:
        gcc = gc.Client(do_version_check=False, login_manager=mock.Mock())
    else:
        gcc = gc.Client(
            asynchronous=asynchronous, do_version_check=False, login_manager=mock.Mock()
        )

    gcc.web_client = mock.MagicMock()

    fake_results = {
        "results": [
            {
                "task_uuid": str(uuid.uuid4()),
                "http_status_code": 200,
            }
        ],
        "task_group_id": str(uuid.uuid4()),
    }
    gcc.web_client.submit = mock.MagicMock(return_value=fake_results)
    gcc.run(endpoint_id=str(uuid.uuid4()), function_id=str(uuid.uuid4()))

    assert gcc.web_client.submit.called
    submit_data = gcc.web_client.submit.call_args[0][0]
    assert "create_websocket_queue" in submit_data
    if asynchronous:
        assert submit_data["create_websocket_queue"] is True
    else:
        assert submit_data["create_websocket_queue"] is False


def test_build_container(mocker, login_manager):
    mock_data = mocker.Mock()
    mock_data.data = {"container_id": "123-456"}
    login_manager.get_web_client.post = mocker.Mock(return_value=mock_data)
    gcc = gc.Client(do_version_check=False, login_manager=login_manager)
    spec = ContainerSpec(
        name="MyContainer",
        pip=[
            "matplotlib==3.5.1",
            "numpy==1.18.5",
        ],
        python_version="3.8",
        payload_url="https://github.com/funcx-faas/funcx-container-service.git",
    )

    container_id = gcc.build_container(spec)
    assert container_id == "123-456"
    login_manager.get_web_client.post.assert_called()
    calls = login_manager.get_web_client.post.call_args
    assert calls[0][0] == "containers/build"
    assert calls[1] == {"data": spec.to_json()}


def test_container_build_status(mocker, login_manager, randomstring):
    expected_status = randomstring()

    class MockData(dict):
        def __init__(self):
            self["status"] = expected_status
            self.http_status = 200

    login_manager.get_web_client.get = mocker.Mock(return_value=MockData())
    gcc = gc.Client(do_version_check=False, login_manager=login_manager)
    status = gcc.get_container_build_status("123-434")
    assert status == expected_status


def test_container_build_status_not_found(mocker, login_manager):
    class MockData(dict):
        def __init__(self):
            self.http_status = 404

    login_manager.get_web_client.get = mocker.Mock(return_value=MockData())
    gcc = gc.Client(do_version_check=False, login_manager=login_manager)

    with pytest.raises(ValueError) as excinfo:
        gcc.get_container_build_status("123-434")

    assert excinfo.value.args[0] == "Container ID 123-434 not found"


def test_container_build_status_failure(mocker, login_manager):
    class MockData(dict):
        def __init__(self):
            self.http_status = 500
            self.http_reason = "This is a reason"

    login_manager.get_web_client.get = mocker.Mock(return_value=MockData())
    gcc = gc.Client(do_version_check=False, login_manager=login_manager)

    with pytest.raises(SystemError) as excinfo:
        gcc.get_container_build_status("123-434")

    assert type(excinfo.value) == SystemError
