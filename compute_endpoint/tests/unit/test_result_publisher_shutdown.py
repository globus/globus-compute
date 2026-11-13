from __future__ import annotations

from unittest import mock

import pytest
from globus_compute_endpoint.endpoint.rabbit_mq import (
    RabbitPublisherStatus,
    ResultPublisher,
)


def test_rp_publishes_outstanding_results_before_stopping():
    rp = ResultPublisher(cred_fn=dict)
    rp._mq_conn = mock.Mock()
    rp._mq_chan = mock.Mock()
    rp.status = RabbitPublisherStatus.connected
    rp._publish_kwargs = {"exchange": "results"}
    rp.is_alive = mock.Mock(return_value=True)
    rp._stop_ioloop = mock.Mock()

    future = rp.publish(b"a failed task's result")
    rp._stop_event.set()
    rp._event_watcher()

    rp._mq_chan.basic_publish.assert_called_once_with(
        body=b"a failed task's result", exchange="results"
    )
    assert rp._outstanding.empty()
    assert rp._awaiting_confirmation == {1: future}
    assert not future.done()


def test_rp_fails_queued_results_if_stopping_while_disconnected():
    rp = ResultPublisher(cred_fn=dict)
    rp._mq_conn = mock.Mock()
    rp._mq_chan = None
    rp.status = RabbitPublisherStatus.closed
    rp.is_alive = mock.Mock(return_value=True)
    rp._stop_ioloop = mock.Mock()

    future = rp.publish(b"result")
    rp._stop_event.set()
    rp._event_watcher()

    assert rp._outstanding.empty()
    with pytest.raises(RuntimeError, match="stopped before the message"):
        future.result()


def test_rp_waits_for_publish_confirmations_before_closing():
    rp = ResultPublisher(cred_fn=dict)
    rp._mq_conn = mock.Mock()
    rp._shutdown_deadline = 10_000_000_000
    rp._awaiting_confirmation[1] = mock.Mock()
    rp._awaiting_confirmation[1].done.return_value = False

    with mock.patch(
        "globus_compute_endpoint.endpoint.rabbit_mq.result_publisher.time.monotonic",
        return_value=1,
    ):
        rp._stop_ioloop()

    rp._mq_conn.ioloop.call_later.assert_called_once_with(0.1, rp._stop_ioloop)
    rp._mq_conn.close.assert_not_called()
