from funcx import FuncXExecutor


def test_executor_shutdown(mocker):
    mocker.patch("funcx.sdk.executor.atexit")
    mocker.patch("funcx.sdk.executor.ExecutorPollerThread.event_loop_thread")
    fcli = mocker.MagicMock()
    fexe = FuncXExecutor(funcx_client=fcli)
    fexe.poller_thread.atomic_controller.increment()
    fexe.poller_thread.atomic_controller.decrement()

    fexe.shutdown()  # No crashing, please.
