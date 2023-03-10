from __future__ import annotations

import os
import queue
import random
import threading
from unittest import mock

import globus_compute_endpoint.endpoint.rabbit_mq.command_queue_subscriber as cqs
import pytest as pytest
from pika.spec import Basic, BasicProperties
from tests.utils import try_assert

_MOCK_BASE = "globus_compute_endpoint.endpoint.rabbit_mq.command_queue_subscriber."


class MockedCommandQueueSubscriber(cqs.CommandQueueSubscriber):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._time_to_stop_mock = threading.Event()

    def start(self) -> None:
        super().start()
        try_assert(lambda: self._connection is not None)

    def run(self):
        self._connection = mock.MagicMock()
        self._channel = mock.MagicMock()
        self._time_to_stop_mock.wait()  # simulate thread work

    def join(self, timeout: float | None = None) -> None:
        if self._stop_event.is_set():  # important to identify bugs
            self._time_to_stop_mock.set()
        super().join(timeout=timeout)


@pytest.fixture
def mock_cqs():
    q = {"queue": None}
    mq = mock.Mock(spec=queue.SimpleQueue)
    se = threading.Event()

    mcqs = MockedCommandQueueSubscriber(queue_info=q, command_queue=mq, stop_event=se)
    mcqs.start()

    yield q, mq, se, mcqs

    mcqs._stop_event.set()
    if mcqs.is_alive():
        mcqs.join()


def test_cqs_repr():
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=None, stop_event=None
    )
    assert "<✗;" in repr(cq)
    cq._consumer_tag = "asdf"
    assert "<✓;" in repr(cq)
    cq._consumer_tag = None
    assert "<✗;" in repr(cq)

    assert repr(cq).startswith(cq.__class__.__name__)
    assert f"pid={os.getpid()}" in repr(cq)


def test_cqs_stops_if_unable_to_connect(mocker):
    mock_rnd = mocker.patch(f"{_MOCK_BASE}random")
    mock_rnd.uniform.return_value = 0  # don't wait during test
    mock_stop = mock.Mock(spec=threading.Event)
    mock_stop.is_set.return_value = False
    mock_stop.wait.return_value = None
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=None, stop_event=mock_stop
    )
    cq._connect = mock.Mock()

    cq.run()
    assert mock_rnd.uniform.call_count == cq.connect_attempt_limit - 1, "Random idle"
    assert cq._connection_tries >= cq.connect_attempt_limit
    assert cq._stop_event.wait.call_count == cq.connect_attempt_limit, "Should idle"


def test_cqs_gracefully_handles_unexpected_exception(mocker):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mock_rnd = mocker.patch(f"{_MOCK_BASE}random")
    mock_rnd.uniform.return_value = 0  # don't wait during test
    mock_stop = mock.Mock(spec=threading.Event)
    mock_stop.is_set.return_value = False
    mock_stop.wait.return_value = None
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=None, stop_event=mock_stop
    )
    cq._connect = mock.Mock()
    cq._event_watcher = mock.Mock(side_effect=Exception)

    cq.run()

    assert mock_log.exception.call_count == cq.connect_attempt_limit
    args, _kwargs = mock_log.exception.call_args
    assert "shutting down" in args[0]


def test_on_message_puts_to_queue(randomstring):
    mock_q = mock.Mock(spec=queue.SimpleQueue)
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=mock_q, stop_event=None
    )
    mock_chan = mock.Mock()
    bd = Basic.Deliver(delivery_tag=random.randint(1, 1000))
    props = BasicProperties()
    body = randomstring().encode()
    cq._on_message(mock_chan, bd, props, body)

    assert mock_q.put.called
    assert mock_q.put.call_args[0][0] == (bd.delivery_tag, props, body)


def test_on_message_gracefully_handles_garbled_packet(mocker):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=None, stop_event=None
    )
    cq._on_message(None, None, None, None)

    assert mock_log.debug.called
    assert "Invalid Basic.Deliver" in mock_log.debug.call_args[0][0]


def test_on_message_nacks_on_failure(mocker):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=None, stop_event=None
    )
    mock_chan = mock.Mock()
    bd = Basic.Deliver(delivery_tag=random.randint(1, 1000))
    cq._on_message(mock_chan, bd, None, None)

    assert mock_log.exception.called
    assert "put failed" in mock_log.exception.call_args[0][0]
    assert mock_chan.basic_nack.called
    assert bd.delivery_tag == mock_chan.basic_nack.call_args[0][0]


@pytest.mark.parametrize("exc", (MemoryError("some description"), "some description"))
def test_cqs_stops_loop_on_open_failure(mocker, mock_cqs, exc):
    *_, mcqs = mock_cqs
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    assert not mcqs._connection.ioloop.stop.called, "Test setup verification"

    while not mcqs._stop_event.is_set():
        mcqs._connection_tries += 1

        assert not mock_log.warning.called
        mcqs._connection.ioloop.stop.reset_mock()
        mock_log.debug.reset_mock()

        mcqs._on_open_failed(mcqs._connection, exc)  # kernel of test

        assert mcqs._connection.ioloop.stop.called
        assert mock_log.debug.called
        (fmt, *args), *_ = mock_log.debug.call_args
        msg = fmt % tuple(args)
        assert "Failed to open connection" in msg
        assert str(exc) in msg
        assert exc.__class__.__name__ in msg

    assert mcqs._connection_tries == mcqs.connect_attempt_limit
    assert mock_log.warning.called, "Expected warning only on watcher quit"


def test_cqs_connection_closed_stops_loop(mock_cqs):
    exc = MemoryError("some description")
    *_, mcqs = mock_cqs
    assert not mcqs._connection.ioloop.stop.called
    mcqs._on_connection_closed(mcqs._connection, exc)
    assert mcqs._connection.ioloop.stop.called


def test_cqs_channel_closed_retries_then_shuts_down(mock_cqs):
    exc = Exception("some pika reason")
    *_, mcqs = mock_cqs

    for i in range(1, mcqs.channel_close_window_limit):
        mcqs._connection.ioloop.call_later.reset_mock()
        mcqs._on_channel_closed(mcqs._connection, exc)
        assert len(mcqs._channel_closes) == i
    mcqs._on_channel_closed(mcqs._connection, exc)

    # and finally, no error if called "too many" times
    mcqs._on_channel_closed(mcqs._connection, exc)


def test_cqs_stable_connection_resets_fail_counter(mocker, mock_cqs):
    mock_time = mocker.patch(f"{_MOCK_BASE}time")
    mock_time.time.side_effect = [1000, 1061]  # 60 seconds passed
    *_, mcqs = mock_cqs

    mcqs._connection_tries = 57
    mcqs._start_consuming()
    mcqs._event_watcher()
    assert mcqs._connection_tries == 0


def test_cqs_channel_opened_starts_consuming(mock_cqs):
    mock_channel = mock.Mock()
    *_, mcqs = mock_cqs

    assert mcqs._consumer_tag is None
    mcqs._on_channel_open(mock_channel)
    assert mock_channel is mcqs._channel
    assert mcqs._consumer_tag is not None  # kernel: basic_consume() returns tag


def test_cqs_start_consuming_error_shutsdown(mock_cqs):
    *_, mcqs = mock_cqs

    assert not mcqs._connection.ioloop.call_later.called
    assert mcqs._consumer_tag is None
    mcqs._channel.is_open = False  # not strictly necessary, but increase coverage
    mcqs._channel.is_closed = False  # not strictly necessary, but increase coverage
    mcqs._channel.basic_consume.side_effect = ValueError("Silly Goose!")
    mcqs._start_consuming()
    assert mcqs._connection.ioloop.call_later.called
    assert mcqs._consumer_tag is None


def test_cqs_amqp_acks_in_bulk(mock_cqs):
    *_, mcqs = mock_cqs

    num_messages = random.randint(1, 200)
    for i in range(num_messages):
        mcqs.ack(i)
    assert mcqs._to_ack.qsize() == num_messages, "Verify test setup"
    assert mcqs._channel.basic_ack.call_count == 0
    mcqs._event_watcher()
    assert mcqs._to_ack.qsize() == 0
    assert mcqs._channel.basic_ack.call_count == 1


def test_event_watcher_initiates_shutdown(mock_cqs):
    *_, stop_event, mcqs = mock_cqs

    stop_event.set()
    assert not mcqs._channel.close.called, "Verify test setup"
    mcqs._event_watcher()
    assert mcqs._channel.close.called
