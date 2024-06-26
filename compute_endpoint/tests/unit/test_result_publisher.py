from __future__ import annotations

import random
import threading
from concurrent.futures import Future
from unittest import mock

import pika
import pika.exceptions
import pytest
from globus_compute_endpoint.endpoint.rabbit_mq import (
    RabbitPublisherStatus,
    ResultPublisher,
)

_MOCK_BASE = "globus_compute_endpoint.endpoint.rabbit_mq.result_publisher."
q_info = {"queue_publish_kwargs": {"exchange": "results"}, "exchange": "results"}


@pytest.fixture
def mock_log(mocker):
    yield mocker.patch(f"{_MOCK_BASE}log", autospec=True)


def test_rp_callbacks_hooked_up(mocker, randomstring):
    mock_pika = mocker.patch(f"{_MOCK_BASE}pika")
    queue_info = {"queue": randomstring(), **q_info, "connection_url": "amqp:///"}
    rp = ResultPublisher(queue_info=queue_info)
    rp._connect()

    assert mock_pika.SelectConnection.called

    _a, k = mock_pika.SelectConnection.call_args
    assert "on_open_callback" in k, "Verify successful connection open handled"
    assert "on_close_callback" in k, "Verify connection close handled"
    assert "on_open_error_callback" in k, "Verify connection error handled"


def test_rp_verifies_exchange(randomstring):
    mock_channel = mock.Mock()
    queue_info = {"exchange": randomstring(), **q_info}

    rp = ResultPublisher(queue_info=queue_info)
    rp._on_channel_open(mock_channel)

    assert mock_channel.exchange_declare.called
    _a, k = mock_channel.exchange_declare.call_args
    assert k["passive"] is True, "endpoint does not create exchange"
    assert k["exchange"] == queue_info["exchange"]


def test_rp_verifies_queue(randomstring):
    mock_channel = mock.Mock()
    queue_info = {"queue": randomstring(), **q_info}

    rp = ResultPublisher(queue_info=queue_info)
    rp._mq_chan = mock_channel
    rp._on_exchange_verified(None)

    assert mock_channel.queue_declare.called
    _a, k = mock_channel.queue_declare.call_args
    assert k["passive"] is True, "endpoint should not create queue"
    assert k["queue"] == queue_info["queue"]


@pytest.mark.parametrize("attempt_limit", (random.randint(2, 50),))
def test_rp_stops_if_unable_to_connect(mocker, randomstring, attempt_limit):
    mock_rnd = mocker.patch(f"{_MOCK_BASE}random", spec=random)
    mock_rnd.uniform.return_value = 0  # don't wait during test
    mock_stop = mock.Mock(spec=threading.Event)
    mock_stop.is_set.return_value = False
    mock_stop.wait.return_value = None
    queue_info = {"queue": randomstring(), **q_info}

    rp = ResultPublisher(queue_info=queue_info, connect_attempt_limit=attempt_limit)
    rp._connect = mock.Mock(spec=ResultPublisher._connect)
    rp._stop_event = mock_stop
    rp.run()
    assert mock_rnd.uniform.call_count == attempt_limit - 1, "Random idle"
    assert rp._connection_tries >= attempt_limit
    assert rp._stop_event.wait.call_count == attempt_limit, "Should idle"


def test_rp_connect_limit_very_high_sc30467(randomstring):
    queue_info = {"queue": randomstring(), **q_info}
    rp = ResultPublisher(queue_info=queue_info)
    assert (
        rp.connect_attempt_limit >= 5000
    ), "Some very high limit as RMQ can be down for awhile; SC-30467"


def test_rp_new_channel_resets_delivery_index():
    mock_channel = mock.Mock()
    queue_info = {"exchange": "some_exchange", **q_info}
    rp = ResultPublisher(queue_info=queue_info)
    rp._delivery_tag_index = random.randint(-1000, 1000)
    rp._on_channel_open(mock_channel)

    assert rp._delivery_tag_index == 0


def test_rp_delivery_confirmation_enabled():
    mock_channel = mock.Mock()
    queue_info = {**q_info}
    rp = ResultPublisher(queue_info=queue_info)
    rp._mq_chan = mock_channel

    rp._on_queue_verified(None)

    assert mock_channel.confirm_delivery.called
    a, _k = mock_channel.confirm_delivery.call_args
    assert a[0] == rp._on_delivery


def test_rp_channel_closed_retries_then_shuts_down(mock_log, randomstring):
    exc = Exception(f"some reason: {randomstring()}")
    queue_info = {**q_info}
    rp = ResultPublisher(queue_info=queue_info)
    rp._mq_chan = mock.Mock()
    rp._mq_conn = mock.Mock()

    for i in range(1, rp.channel_close_window_limit):
        mock_log.warning.reset_mock()
        rp._mq_conn.ioloop.call_later.reset_mock()
        rp._on_channel_closed(rp._mq_conn, exc)
        assert len(rp._channel_closes) == i

        assert not mock_log.error.called, "Not an outright error until death"
        assert mock_log.warning.called
        a, _k = mock_log.warning.call_args
        assert "Channel closed" in a[0]
        assert str(exc) in a[0]

    rp._on_channel_closed(rp._mq_conn, exc)  # final time
    assert mock_log.error.called, "Too many failures *is* an error"
    a, _k = mock_log.error.call_args
    assert "Unable to sustain channel" in a[0]
    assert f"after {len(rp._channel_closes)} attempts" in a[0]
    assert f"in {rp.channel_close_window_limit} seconds" in a[0]

    assert rp._stop_event.is_set()

    # and finally, no error if called "too many" times
    rp._on_channel_closed(rp._mq_conn, exc)


def test_rp_stable_connection_resets_fail_counter(mocker):
    mock_time = mocker.patch(f"{_MOCK_BASE}time")
    mock_time.time.side_effect = [1000, 1061]  # 60 seconds passed
    queue_info = {**q_info}
    rp = ResultPublisher(queue_info=queue_info)
    rp._mq_chan = mock.Mock()
    rp._mq_conn = mock.Mock()

    rp._connection_tries = 57
    rp._on_queue_verified(None)
    rp._event_watcher()
    assert rp._connection_tries == 0


def test_rp_stops_trying_on_unrecoverable(randomstring, mock_log):
    exc_text = randomstring()
    exc = pika.exceptions.ProbableAuthenticationError(f"some reason: {exc_text}")

    mock_conn = mock.Mock(spec=pika.BaseConnection)
    queue_info = {"queue": randomstring(), **q_info}

    attempt_limit = random.randint(1_000_000, 2_000_000)  # something large
    rp = ResultPublisher(queue_info=queue_info, connect_attempt_limit=attempt_limit)
    rp._on_open_failed(mock_conn, exc)
    assert rp._connection_tries >= attempt_limit, "Expect signal to event loop"

    a, _k = mock_log.error.call_args
    assert "[invalid credentials; unrecoverable]" in a[0]
    assert exc_text in a[0]


def test_rp_handles_bulk_ack_deliveries():
    queue_info = {**q_info}
    rp = ResultPublisher(queue_info=queue_info)
    multiple_dtag = rp._delivery_tag_index + random.randint(5, 30)
    mock_frame = mock.Mock()
    mock_frame.method.INDEX = pika.spec.Basic.Ack.INDEX
    mock_frame.method.multiple = True
    mock_frame.method.delivery_tag = multiple_dtag

    delivered = {
        dtag: Future() for dtag in range(rp._delivery_tag_index, multiple_dtag + 1)
    }
    rp._awaiting_confirmation.update(delivered)
    rp._awaiting_confirmation[multiple_dtag + 1] = Future()
    assert len(rp._awaiting_confirmation) == multiple_dtag - rp._delivery_tag_index + 2
    rp._on_delivery(mock_frame)
    assert len(rp._awaiting_confirmation) == 1

    dtag, f = rp._awaiting_confirmation.popitem()  # should be only one, per +2 (above)
    assert dtag == multiple_dtag + 1, "only ack up to received frame"
    assert not f.done()

    assert all(f.done() for f in delivered.values())
    assert all(f.result() is None for f in delivered.values())


def test_rp_handles_bulk_nack_deliveries():
    queue_info = {**q_info}
    rp = ResultPublisher(queue_info=queue_info)
    multiple_dtag = rp._delivery_tag_index + random.randint(5, 30)
    mock_frame = mock.Mock()
    mock_frame.method.INDEX = pika.spec.Basic.Nack.INDEX
    mock_frame.method.multiple = True
    mock_frame.method.delivery_tag = multiple_dtag

    delivered = {
        dtag: Future() for dtag in range(rp._delivery_tag_index, multiple_dtag + 1)
    }
    rp._awaiting_confirmation.update(delivered)
    rp._awaiting_confirmation[multiple_dtag + 1] = Future()
    assert len(rp._awaiting_confirmation) == multiple_dtag - rp._delivery_tag_index + 2
    rp._on_delivery(mock_frame)
    assert len(rp._awaiting_confirmation) == 1

    dtag, f = rp._awaiting_confirmation.popitem()  # should be only one, per +2 (above)
    assert dtag == multiple_dtag + 1, "only nack up to received frame"
    assert not f.done()

    assert all(f.done() for f in delivered.values())
    assert all(f.exception() is not None for f in delivered.values())


def test_rp_event_watcher_publishes(randomstring):
    queue_info = {**q_info}
    rp = ResultPublisher(queue_info=queue_info)
    rp._connection = mock.Mock()
    rp._mq_conn = mock.Mock()
    rp._mq_chan = mock.Mock()
    rp.is_alive = mock.Mock()
    rp.status = RabbitPublisherStatus.connected

    data = [randomstring().encode() for _ in range(random.randint(1, 30))]
    futs = [rp.publish(msg) for msg in data]

    rp._event_watcher()

    assert rp._mq_chan.basic_publish.call_count == len(data)
    assert rp._delivery_tag_index == len(data)
    assert len(rp._awaiting_confirmation) == len(data)
    assert rp._total_published == len(data)
    assert rp._mq_conn.ioloop.call_later.called, "expect event loop re-armed"

    # test round-trip-edness
    mock_frame = mock.Mock()
    mock_frame.method.INDEX = pika.spec.Basic.Ack.INDEX
    mock_frame.method.multiple = True
    mock_frame.method.delivery_tag = rp._delivery_tag_index
    rp._on_delivery(mock_frame)

    assert all(f.done() for f in futs)


def test_rp_publish_enqueues_message(randomstring):
    queue_info = {**q_info}
    rp = ResultPublisher(queue_info=queue_info)
    rp._mq_chan = mock.Mock()
    rp.is_alive = mock.Mock()

    msg = b"abc"
    f = rp.publish(msg)

    assert (
        not rp._awaiting_confirmation
    ), "publish() *enqueues*, but doesn't upstream publish yet"

    f_enqueued, msg_enqueued = rp._outstanding.get()

    assert msg == msg_enqueued
    assert not f.done(), "Ensure a future"

    res = randomstring()
    f_enqueued.set_result(res)
    assert f.result() == res


@pytest.mark.parametrize("attempt_limit", (random.randint(3, 50),))
def test_rp_on_connection_closed_logs(mock_log, attempt_limit):
    assert attempt_limit > 2, "At least 3 states tested"

    exc = pika.exceptions.ChannelClosedByClient(200, "Some text")
    cq = ResultPublisher(queue_info={**q_info})
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
