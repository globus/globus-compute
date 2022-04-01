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
        future_id=fut_id, function_id=func_id, endpoint_id=ep_id, args=(), kwargs={}
    )
    as_str = str(info)
    assert as_str.startswith("TaskSubmissionInfo(")
    assert as_str.endswith("args=..., kwargs=...)")
    assert "future_id=10" in as_str
    assert "function_id='foo_func'" in as_str
    assert "endpoint_id='bar_ep'" in as_str
