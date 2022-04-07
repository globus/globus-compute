import asyncio
import json
import random
import uuid

import pytest
import websockets.exceptions

from funcx.sdk.asynchronous.ws_polling_task import WebSocketPollingTask
from funcx.sdk.executor import AtomicController, FuncXFuture
from funcx.serialize import FuncXSerializer


def _start():
    pass


def _stop():
    pass


def test_close_with_null_ws_state(mocker):
    eventloop = asyncio.new_event_loop()
    wspt = WebSocketPollingTask(
        funcx_client=mocker.MagicMock(),
        loop=eventloop,
        atomic_controller=AtomicController(_start, _stop),
        auto_start=False,
    )
    eventloop.run_until_complete(wspt.close())  # No crashing, please
    eventloop.close()


def test_external_disconnect(mocker):
    async def mock_recv():
        raise websockets.exceptions.ConnectionClosedOK(1000, "Some Reason")

    eventloop = asyncio.new_event_loop()
    wspt = WebSocketPollingTask(
        funcx_client=mocker.MagicMock(),
        loop=eventloop,
        atomic_controller=AtomicController(_start, _stop),
        auto_start=False,
    )
    wspt._ws = mocker.MagicMock()
    wspt._ws.recv = mock_recv
    time_to_disconnect = eventloop.run_until_complete(
        wspt.handle_incoming(pending_futures={})
    )
    eventloop.close()

    assert not time_to_disconnect


def test_unknown_results_gracefully_kept(mocker):
    num_results = random.randint(0, 10)
    mock_data = [(str(uuid.uuid4()), result) for result in range(num_results)]
    async_data_iter = iter(mock_data)

    async def mock_recv():
        try:
            t_id, result = next(async_data_iter)
            return json.dumps({"task_id": t_id, "result": result})
        except StopIteration:
            pass
        raise websockets.exceptions.ConnectionClosedOK(1000, "Some Reason")

    eventloop = asyncio.new_event_loop()
    wspt = WebSocketPollingTask(
        funcx_client=mocker.MagicMock(),
        loop=eventloop,
        atomic_controller=AtomicController(_start, _stop),
        auto_start=False,
    )
    wspt._ws = mocker.MagicMock()
    wspt._ws.recv = mock_recv
    eventloop.run_until_complete(wspt.handle_incoming(pending_futures={}))
    eventloop.close()
    for task_id, _ in mock_data:
        assert task_id in wspt.unknown_results


def test_previously_unknown_results_handled(mocker):
    num_results = random.randint(0, 10)
    mock_data = [(str(uuid.uuid4()), result) for result in range(num_results)]
    async_data_iter = iter(mock_data)

    async def mock_recv():
        try:
            t_id, _ = next(async_data_iter)
            return json.dumps({"task_id": t_id})
        except StopIteration:
            pass
        raise websockets.exceptions.ConnectionClosedOK(1000, "Some Reason")

    serde = FuncXSerializer()
    futures = {task_id: FuncXFuture(task_id) for task_id, _ in mock_data}
    pending_futures = {f.task_id: f for f in futures.values()}
    fxclient = mocker.MagicMock()
    fxclient.fx_serializer = serde
    eventloop = asyncio.new_event_loop()
    wspt = WebSocketPollingTask(
        funcx_client=fxclient,
        loop=eventloop,
        atomic_controller=AtomicController(_start, _stop),
        auto_start=False,
    )
    wspt.unknown_results = {
        task_id: {"task_id": task_id, "result": serde.serialize(result)}
        for task_id, result in mock_data
    }
    wspt._ws = mocker.MagicMock()
    wspt._ws.recv = mock_recv
    eventloop.run_until_complete(wspt.handle_incoming(pending_futures))
    eventloop.close()
    assert not pending_futures
    for task_id, result in mock_data:
        assert futures[task_id].result() == result


def test_invalid_datagram_structure_gracefully_sets_exception(mocker):
    mock_data = [
        (str(uuid.uuid4()), "abc"),
        (str(uuid.uuid4()), "def"),
    ]
    async_data_iter = iter(mock_data)

    async def mock_recv():
        try:
            t_id, result = next(async_data_iter)
            return json.dumps({"task_id": t_id})
        except StopIteration:
            pass
        raise websockets.exceptions.ConnectionClosedOK(1000, "Some Reason")

    futures = {task_id: FuncXFuture(task_id) for task_id, _ in mock_data}
    pending_futures = {f.task_id: f for f in futures.values()}
    fxclient = mocker.MagicMock()
    fxclient.fx_serializer = FuncXSerializer()

    eventloop = asyncio.new_event_loop()
    wspt = WebSocketPollingTask(
        funcx_client=fxclient,
        loop=eventloop,
        atomic_controller=AtomicController(_start, _stop),
        auto_start=False,
    )
    wspt.unknown_results = {
        # intentionally _not_ serialized; that's the test
        task_id: {"task_id": task_id, "result": result}
        for task_id, result in mock_data
    }
    wspt._ws = mocker.MagicMock()
    wspt._ws.recv = mock_recv
    eventloop.run_until_complete(wspt.handle_incoming(pending_futures))
    eventloop.close()

    assert not pending_futures
    for task_id, _ in mock_data:
        with pytest.raises(Exception):
            futures[task_id].result()
