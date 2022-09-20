import random
import uuid

import pytest

from funcx import FuncXExecutor
from funcx.sdk.executor import TaskSubmissionInfo


def test_executor_shutdown(mocker):
    """ensure that executor shutdown does not crash"""
    mocker.patch("funcx.sdk.executor.atexit")
    mocker.patch("funcx.sdk.executor.ExecutorPollerThread.event_loop_thread")
    fcli = mocker.MagicMock()
    fexe = FuncXExecutor(funcx_client=fcli)
    # increment starts the websocket handler, decrement is a no-op
    fexe.poller_thread.atomic_controller.increment()
    fexe.poller_thread.atomic_controller.decrement()

    # there is a handler for the websocket service
    ws_handler = fexe.poller_thread.ws_handler
    assert ws_handler is not None
    # but its underlying connection is not valid because the
    # executor's main thread is mocked
    # the connection should not be gettable: ValueError on property access
    with pytest.raises(ValueError):
        ws_handler.ws

    fexe.shutdown()


def test_task_submission_info_stringification():
    fut_id = 10
    func_id = "foo_func"
    ep_id = "bar_ep"

    info = TaskSubmissionInfo(
        task_num=fut_id, function_id=func_id, endpoint_id=ep_id, args=(), kwargs={}
    )
    as_str = str(info)
    assert as_str.startswith("TaskSubmissionInfo(")
    assert as_str.endswith("args=..., kwargs=...)")
    assert "task_num=10" in as_str
    assert "function_id='foo_func'" in as_str
    assert "endpoint_id='bar_ep'" in as_str


def test_property_results_ws_uri_passthru(mocker, randomstring):
    fcli = mocker.MagicMock(results_ws_uri=randomstring())
    fexe = FuncXExecutor(funcx_client=fcli)
    assert fexe.results_ws_uri is fcli.results_ws_uri

    fcli.results_ws_uri = randomstring()
    assert fexe.results_ws_uri is fcli.results_ws_uri


def test_property_task_group_id_passthru(mocker, randomstring):
    fcli = mocker.MagicMock(session_task_group_id=randomstring())
    fexe = FuncXExecutor(funcx_client=fcli)
    assert fexe.task_group_id is fcli.session_task_group_id

    fcli.session_task_group_id = randomstring()
    assert fexe.task_group_id is fcli.session_task_group_id


@pytest.mark.parametrize("num_tasks", [0, 1, 2, 10])
def test_reload_tasks_happy_path(mocker, num_tasks, randomstring):
    mocker.patch("funcx.sdk.executor.ExecutorPollerThread")
    mock_log = mocker.patch("funcx.sdk.executor.log")

    tg_uuid = str(uuid.uuid4())
    mock_data = {
        "taskgroup_id": tg_uuid,
        "tasks": [
            {"id": randomstring(), "created_at": 1234567890} for _ in range(num_tasks)
        ],
    }

    fcli = mocker.MagicMock()
    fcli.session_task_group_id = tg_uuid
    fcli.web_client.get_taskgroup_tasks.return_value = mock_data
    fexe = FuncXExecutor(funcx_client=fcli)

    client_futures = list(fexe.reload_tasks())
    if num_tasks == 0:
        log_args, log_kwargs = mock_log.warning.call_args
        assert "Received no tasks" in log_args[0]
        assert tg_uuid in log_args[0]
    else:
        assert not mock_log.warning.called

    assert len(client_futures) == num_tasks
    assert fexe.poller_thread.atomic_controller.increment.called
    assert 1 == fexe.poller_thread.atomic_controller.increment.call_count
    assert not any(fut.done() for fut in client_futures)

    for fut in fexe._function_future_map.values():
        fut.set_result(1)

    assert all(fut.done() for fut in client_futures)


def test_reload_tasks_cancels_existing_futures(mocker, randomstring):
    mocker.patch("funcx.sdk.executor.ExecutorPollerThread")

    tg_uuid = str(uuid.uuid4())

    def mock_data():
        return {
            "taskgroup_id": tg_uuid,
            "tasks": [
                {"id": randomstring(), "created_at": 1234567890}
                for i in range(random.randint(0, 20))
            ],
        }

    fcli = mocker.MagicMock()
    fcli.session_task_group_id = tg_uuid
    fcli.web_client.get_taskgroup_tasks.return_value = mock_data()
    fexe = FuncXExecutor(funcx_client=fcli)

    client_futures_1 = list(fexe.reload_tasks())
    fcli.get_taskgroup_tasks.return_value = mock_data()
    client_futures_2 = list(fexe.reload_tasks())

    assert all(fut.done() for fut in client_futures_1)
    assert all(fut.cancelled() for fut in client_futures_1)
    assert not any(fut.done() for fut in client_futures_2)


def test_client_taskgroup_tasks_fails_gracefully(mocker):
    mocker.patch("funcx.sdk.executor.ExecutorPollerThread")

    tg_uuid = str(uuid.uuid4())
    mock_datum = (
        (KeyError, {"mispeleed": tg_uuid}),
        (ValueError, {"taskgroup_id": "abcd"}),
        (None, {"taskgroup_id": tg_uuid}),
    )

    fcli = mocker.MagicMock()
    fcli.session_task_group_id = tg_uuid
    fexe = FuncXExecutor(funcx_client=fcli)

    for expected_exc_class, md in mock_datum:
        fcli.web_client.get_taskgroup_tasks.return_value = md
        if expected_exc_class:
            with pytest.raises(expected_exc_class):
                fexe.reload_tasks()
        else:
            fexe.reload_tasks()
            fexe.shutdown()
