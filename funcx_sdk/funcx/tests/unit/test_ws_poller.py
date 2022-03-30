import asyncio

from funcx.sdk.asynchronous.ws_polling_task import WebSocketPollingTask
from funcx.sdk.executor import AtomicController


def _start():
    pass


def _stop():
    pass


def test_close_with_null_ws_state(mocker):
    fxclient = mocker.MagicMock()
    eventloop = asyncio.new_event_loop()
    wspt = WebSocketPollingTask(
        funcx_client=fxclient,
        loop=eventloop,
        atomic_controller=AtomicController(_start, _stop),
        auto_start=False,
    )
    eventloop.run_until_complete(wspt.close())  # No crashing, please
    eventloop.close()
