import asyncio
import json
import random
import uuid

from globus_compute_sdk.sdk.asynchronous.compute_future import ComputeFuture
from globus_compute_sdk.sdk.asynchronous.ws_polling_task import WebSocketPollingTask
from globus_compute_sdk.sdk.executor import AtomicController


def _start():
    pass


def _stop():
    pass


def test_close_with_null_ws_state(mocker):
    client = mocker.MagicMock()
    eventloop = asyncio.new_event_loop()
    wspt = WebSocketPollingTask(
        funcx_client=client,
        loop=eventloop,
        atomic_controller=AtomicController(_start, _stop),
        auto_start=False,
    )
    eventloop.run_until_complete(wspt.close())  # No crashing, please
    eventloop.close()


def test_polling_task_cancels_futures_upon_upstream_failure(mocker):
    mock_msg = "no task_id exception == no fix; quit!"

    expected_num_good_results = random.randint(0, 10)
    mock_data = [
        {"task_id": str(uuid.uuid4()), "result": i}
        for i in range(expected_num_good_results)
    ]
    mock_data.append({"exception": mock_msg})
    mock_data.extend(
        {"task_id": str(uuid.uuid4()), "result": i}
        for i in range(random.randint(0, 10))
    )
    mock_data_iter = iter(mock_data)
    tids = (md.get("task_id", uuid.uuid4()) for md in mock_data)
    pending_futures = {tid: ComputeFuture(tid) for tid in tids}
    futures = list(pending_futures.values())

    async def mock_recv():
        return json.dumps(next(mock_data_iter))

    client = mocker.MagicMock()
    eventloop = asyncio.new_event_loop()
    wspt = WebSocketPollingTask(
        funcx_client=client,
        loop=eventloop,
        atomic_controller=AtomicController(_start, _stop),
        auto_start=False,
    )
    wspt._ws = mocker.MagicMock()
    wspt._ws.recv = mock_recv
    result = eventloop.run_until_complete(wspt.handle_incoming(pending_futures))
    eventloop.close()

    assert (
        result
    ), "Upstream has _not_ closed the connection -- just indicated unrecoverable error"

    num_good = len(futures) - len(pending_futures)
    assert num_good == expected_num_good_results
    assert any(pf.cancelled() for pf in pending_futures.values())


def test_malformed_response_handled_gracefully(mocker):
    client = mocker.MagicMock()
    eventloop = asyncio.new_event_loop()
    wspt = WebSocketPollingTask(
        funcx_client=client,
        loop=eventloop,
        atomic_controller=AtomicController(_start, _stop),
        auto_start=False,
    )
    task_fut = ComputeFuture()
    data = {"reason": "Jim bob Bonita Mae"}
    eventloop.run_until_complete(wspt.set_result(task_fut, data))
    eventloop.close()

    assert data["reason"] in str(task_fut.exception())
