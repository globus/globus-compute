from __future__ import annotations

import queue
from unittest import mock

import pytest
from globus_compute_endpoint.endpoint.rabbit_mq import TaskQueueSubscriber
from tests.utils import try_assert

_MOCK_BASE = "globus_compute_endpoint.endpoint.rabbit_mq.task_queue_subscriber."
q_info = {"queue_publish_kwargs": {"exchange": "results"}, "exchange": "results"}


@pytest.fixture
def mock_log():
    with mock.patch(f"{_MOCK_BASE}log", autospec=True) as m:
        yield m


@pytest.fixture
def mock_pika():
    with mock.patch(f"{_MOCK_BASE}pika", autospec=True) as m:
        yield m


def test_tqs_as_contextmanager(randomstring, mock_pika):
    queue_info = {"queue": randomstring(), **q_info, "connection_url": "amqp:///"}
    with TaskQueueSubscriber(
        queue_info=queue_info,
        pending_task_queue=queue.SimpleQueue(),
    ) as tqs:
        try_assert(tqs.is_alive, "Context manager starts thread")

    try_assert(lambda: not tqs.is_alive(), "Context manager stops thread")


def test_tqs_callbacks_hooked_up(randomstring, mock_pika):
    queue_info = {"queue": randomstring(), **q_info, "connection_url": "amqp:///"}
    tqs = TaskQueueSubscriber(
        queue_info=queue_info,
        pending_task_queue=queue.SimpleQueue(),
    )
    tqs._connect()

    assert mock_pika.SelectConnection.called

    _a, k = mock_pika.SelectConnection.call_args
    assert "on_open_callback" in k, "Verify successful connection open handled"
    assert "on_close_callback" in k, "Verify connection close handled"
    assert "on_open_error_callback" in k, "Verify connection error handled"
