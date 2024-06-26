from __future__ import annotations

import os
import queue
import random
import threading
from unittest import mock

import globus_compute_endpoint.endpoint.rabbit_mq.command_queue_subscriber as cqs
import pika
import pika.exceptions
import pytest as pytest
from pika.spec import Basic, BasicProperties

_MOCK_BASE = "globus_compute_endpoint.endpoint.rabbit_mq.command_queue_subscriber."


class MockedCommandQueueSubscriber(cqs.CommandQueueSubscriber):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._time_to_stop_mock = threading.Event()
        self._test_mock_cqs_setup_complete = threading.Event()

    def start(self) -> None:
        super().start()
        self._test_mock_cqs_setup_complete.wait(5)
        assert self._test_mock_cqs_setup_complete.is_set()

    def run(self):
        self._connection = mock.MagicMock()
        self._channel = mock.MagicMock()

        self._test_mock_cqs_setup_complete.set()  # let tests know they may continue
        self._time_to_stop_mock.wait()  # simulate thread work

    def join(self, timeout: float | None = None) -> None:
        if self._stop_event.is_set():  # important to identify bugs
            self._time_to_stop_mock.set()
        else:
            raise AssertionError("Expect that CQS thread always stops when test done")

        super().join(timeout=timeout)


@pytest.fixture
def mock_log(mocker):
    yield mocker.patch(f"{_MOCK_BASE}log", autospec=True)


@pytest.fixture
def mock_rnd(mocker):
    mock_rnd = mocker.patch(f"{_MOCK_BASE}random")
    mock_rnd.uniform.return_value = 0  # used in the tested code for sleep; don't wait!
    yield mock_rnd


@pytest.fixture
def mock_cqs():
    q = {"queue": None}
    cal = random.randint(3, 10)
    mq = mock.Mock(spec=queue.SimpleQueue)
    se = threading.Event()

    mcqs = MockedCommandQueueSubscriber(
        queue_info=q, command_queue=mq, stop_event=se, connect_attempt_limit=cal
    )
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


def test_cqs_callbacks_hooked_up(mocker, randomstring):
    mock_pika = mocker.patch(f"{_MOCK_BASE}pika")
    qinfo = {"connection_url": "amqp:///"}
    mock_stop = mock.Mock(spec=threading.Event)
    mock_cqq = mock.Mock(spec=queue.SimpleQueue)
    cq = cqs.CommandQueueSubscriber(
        queue_info=qinfo, command_queue=mock_cqq, stop_event=mock_stop
    )
    cq._connect()

    assert mock_pika.SelectConnection.called

    _a, k = mock_pika.SelectConnection.call_args
    assert "on_open_callback" in k, "Verify successful connection open handled"
    assert "on_close_callback" in k, "Verify connection close handled"
    assert "on_open_error_callback" in k, "Verify connection error handled"


@pytest.mark.parametrize("attempt_limit", (random.randint(1, 50),))
def test_cqs_stops_if_unable_to_connect(mock_rnd, attempt_limit):
    mock_stop = mock.Mock(spec=threading.Event)
    mock_stop.is_set.return_value = False
    mock_stop.wait.return_value = None

    cq = cqs.CommandQueueSubscriber(
        queue_info=None,
        command_queue=None,
        stop_event=mock_stop,
        connect_attempt_limit=attempt_limit,
    )
    cq._connect = mock.Mock(spec=cqs.CommandQueueSubscriber._connect)

    cq.run()
    assert mock_rnd.uniform.call_count == attempt_limit - 1, "Random idle"
    assert cq._connection_tries >= attempt_limit
    assert cq._stop_event.wait.call_count == attempt_limit, "Should idle"


def test_cqs_connect_limit_very_high_sc30467():
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=None, stop_event=None
    )
    assert (
        cq.connect_attempt_limit >= 5000
    ), "Some very high limit as RMQ can be down for awhile; SC-30467"


def test_cqs_gracefully_handles_unexpected_exception(mock_log, mock_rnd):
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


@pytest.mark.parametrize("attempt_limit", (random.randint(10, 20),))
def test_cqs_first_connect_attempts_logged(mock_log, mock_rnd, attempt_limit):
    assert attempt_limit > 1, "This tests at least 2 different log lines"

    mock_log.isEnabledFor.return_value = False

    mock_stop = mock.Mock(spec=threading.Event)
    mock_stop.is_set.return_value = False
    mock_stop.wait.return_value = None

    cq = cqs.CommandQueueSubscriber(
        queue_info=None,
        command_queue=None,
        stop_event=mock_stop,
        connect_attempt_limit=attempt_limit,
    )
    cq._connect = mock.Mock(spec=cqs.CommandQueueSubscriber._connect)
    cq.run()

    count_debug_connects = sum(
        "Opening connection to AMQP service.  Attempt:" in a[0]
        for a, _ in mock_log.debug.call_args_list
    )
    first_connection_logged = sum(
        a[0].endswith("Opening connection to AMQP service.")
        for a, _ in mock_log.info.call_args_list
    )
    second_connection_logged = sum(
        f"Will continue for up to {attempt_limit} attempts." in a[0]
        for a, _ in mock_log.info.call_args_list
    )
    second_log = next(
        a[0]
        for a, _ in mock_log.info.call_args_list
        if f"Will continue for up to {attempt_limit} attempts." in a[0]
    )
    assert count_debug_connects == attempt_limit, "Connection attempts always debugged"
    assert first_connection_logged == 1, "First connection attempt logged (INFO)"
    assert second_connection_logged == 1, "Second log contains connection attempt limit"
    assert "--debug" in second_log, "Second log explains how to get all attempts logged"


def test_cqs_on_message_puts_to_queue(randomstring):
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


def test_cqs_on_message_redacts_credentials(mock_log, randomstring):
    mock_q = mock.Mock(spec=queue.SimpleQueue)
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=mock_q, stop_event=None
    )
    mock_chan = mock.Mock()
    bd = Basic.Deliver(delivery_tag=random.randint(1, 1000))
    props = BasicProperties()
    uname, pword = randomstring(), randomstring()
    body = f"amqps://{uname}:{pword}@somehost...".encode()
    cq._on_message(mock_chan, bd, props, body)

    assert mock_log.debug.called
    a, _k = mock_log.debug.call_args
    msg = a[0] % a[1:]
    assert "Received message from" in msg
    assert "amqps://" in msg
    assert "@somehost" in msg
    assert uname not in msg
    assert pword not in msg
    assert "***:***" in msg


def test_cqs_on_message_gracefully_handles_garbled_packet(mock_log):
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=None, stop_event=None
    )
    cq._on_message(None, None, None, None)

    assert mock_log.debug.called
    assert "Invalid Basic.Deliver" in mock_log.debug.call_args[0][0]


def test_cqs_on_message_nacks_on_failure(mock_log):
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
def test_cqs_stops_loop_on_open_failure(mock_log, mock_cqs, exc):
    *_, mcqs = mock_cqs
    assert not mcqs._connection.ioloop.stop.called, "Test setup verification"

    while not mcqs._stop_event.is_set():
        mcqs._connection_tries += 1

        assert not mock_log.warning.called
        mcqs._connection.ioloop.stop.reset_mock()
        mock_log.debug.reset_mock()

        mcqs._on_open_failed(mcqs._connection, exc)  # kernel of test

        if mcqs._connection_tries == 1:
            assert mock_log.warning.called, "For log clutter, expect only one warning"
            mock_log.warning.reset_mock()

        assert mcqs._connection.ioloop.stop.called
        assert mock_log.debug.called
        (fmt, *args), *_ = mock_log.debug.call_args
        msg = fmt % tuple(args)
        assert "Failed to open connection" in msg
        assert str(exc) in msg
        assert exc.__class__.__name__ in msg

    assert mcqs._connection_tries == mcqs.connect_attempt_limit
    assert mock_log.error.called, "Expected warning only on watcher quit"


def test_cqs_connection_closed_stops_loop(mock_cqs):
    exc = MemoryError("some description")
    *_, mcqs = mock_cqs
    assert not mcqs._connection.ioloop.stop.called
    mcqs._on_connection_closed(mcqs._connection, exc)
    assert mcqs._connection.ioloop.stop.called


@pytest.mark.parametrize("attempt_limit", (random.randint(3, 50),))
def test_cqs_on_connection_closed_logs(mock_log, attempt_limit):
    assert attempt_limit > 2, "At least 3 states tested"

    exc = pika.exceptions.ChannelClosedByClient(200, "Some text")
    cq = cqs.CommandQueueSubscriber(
        queue_info=None, command_queue=None, stop_event=None
    )
    mock_conn = mock.Mock(spec=pika.BaseConnection)
    assert not mock_log.debug.called, "Verify test setup"
    cq._on_connection_closed(mock_conn, exc)

    assert mock_log.debug.called
    assert not mock_log.info.called, "Healthy shutdown not concerning"
    assert not mock_log.warning.called, "Healthy shutdown not concerning"

    mock_log.reset_mock()
    cq._connection_tries += 1  # now at == 1
    cq._on_connection_closed(mock_conn, exc)
    assert mock_log.debug.called, "Debug always emitted"
    assert mock_log.info.called, "Inform user after unexpected close"
    assert mock_log.warning.called, "Inform user can't sustain connection"

    for _i in range(attempt_limit - 2):
        mock_log.reset_mock()
        cq._connection_tries += 1  # now at > 1
        cq._on_connection_closed(mock_conn, exc)
        assert mock_log.debug.called, "Debug always emitted"
        assert not mock_log.info.called, "Log informed; don't spam it"
        assert not mock_log.warning.called, "Log informed; don't spam it"


def test_cqs_channel_closed_retries_then_shuts_down(mock_log, mock_cqs, randomstring):
    exc_text = f"some pika reason {randomstring()}"
    exc = Exception(exc_text)
    *_, mcqs = mock_cqs

    for i in range(1, mcqs.channel_close_window_limit):
        mcqs._connection.ioloop.call_later.reset_mock()
        mcqs._on_channel_closed(mcqs._channel, exc)
        assert len(mcqs._channel_closes) == i
        a, _k = mock_log.warning.call_args
        assert " Channel closed " in a[0]
        assert exc_text in a[0], "Expect exception text in logs"
    assert mock_log.debug.call_count == i
    assert mock_log.warning.call_count == i
    assert not mock_log.error.called

    mcqs._on_channel_closed(mcqs._connection, exc)
    assert mock_log.error.called
    a, _k = mock_log.error.call_args
    assert " Unable to sustain channel " in a[0], "Expect error log after attempts"
    assert exc_text in a[0], "Expect exception text in logs"

    # and finally, no error if called "too many" times
    mcqs._on_channel_closed(mcqs._connection, exc)
    assert mock_log.error.call_count == 2

    assert mock_log.debug.call_count == i, "After attempts, should be only errors"
    assert mock_log.warning.call_count == i, "After attempts, should be only errors"


def test_cqs_stable_connection_resets_fail_counter(mocker, mock_cqs):
    mock_time = mocker.patch(f"{_MOCK_BASE}time")
    mock_time.time.side_effect = [1000, 1061]  # 60 seconds passed
    *_, mcqs = mock_cqs

    mcqs._connection_tries = 57
    mcqs._start_consuming()
    mcqs._event_watcher()
    assert mcqs._connection_tries == 0


def test_cqs_stops_trying_on_unrecoverable(randomstring, mock_log):
    exc_text = randomstring()
    exc = pika.exceptions.ProbableAuthenticationError(f"some reason: {exc_text}")

    mock_conn = mock.Mock(spec=pika.BaseConnection)
    mock_stop = mock.Mock(spec=threading.Event)

    attempt_limit = random.randint(1_000_000, 2_000_000)  # something large
    cq = cqs.CommandQueueSubscriber(
        queue_info=None,
        command_queue=None,
        stop_event=mock_stop,
        connect_attempt_limit=attempt_limit,
    )
    cq._on_open_failed(mock_conn, exc)
    assert cq._connection_tries >= attempt_limit, "Expect signal to event loop"
    assert mock_stop.set.called, "Expect signal to event loop"

    a, _k = mock_log.error.call_args
    assert "[invalid credentials; unrecoverable]" in a[0]
    assert exc_text in a[0]


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


def test_cqs_event_watcher_initiates_shutdown(mock_cqs):
    *_, stop_event, mcqs = mock_cqs

    stop_event.set()
    assert not mcqs._channel.close.called, "Verify test setup"
    mcqs._event_watcher()
    assert mcqs._channel.close.called
