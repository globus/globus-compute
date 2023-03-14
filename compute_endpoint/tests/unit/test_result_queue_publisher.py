import inspect
import random
from unittest.mock import MagicMock

import pika
from globus_compute_endpoint.endpoint.rabbit_mq import (
    RabbitPublisherStatus,
    ResultQueuePublisher,
)


def test_rqp_verifies_provided_queue_info(mocker, randomstring):
    mock_conn = MagicMock()
    mock_channel = MagicMock()
    mock_pika = mocker.patch(
        "globus_compute_endpoint.endpoint.rabbit_mq.result_queue_publisher.pika"
    )
    mock_pika.BlockingConnection.return_value = mock_conn
    mock_conn.channel.return_value = mock_channel
    queue_info = {
        "connection_url": randomstring(),
        "exchange": randomstring(),
        "queue": randomstring(),
        "queue_publish_kwargs": {},
    }

    rqp = ResultQueuePublisher(queue_info=queue_info)
    assert rqp.status is RabbitPublisherStatus.closed

    rqp.connect()
    assert rqp.status is RabbitPublisherStatus.connected

    mock_channel.exchange_declare.assert_called()
    args, kwargs = mock_channel.exchange_declare.call_args
    assert kwargs["passive"] is True, "endpoint should not create exchange"
    assert kwargs["exchange"] == queue_info["exchange"]

    mock_channel.queue_declare.assert_called()
    args, kwargs = mock_channel.queue_declare.call_args
    assert kwargs["passive"] is True, "endpoint should not create queue"
    assert kwargs["queue"] == queue_info["queue"]


def test_rqp_uses_provided_publish_args(randomstring):
    queue_info = {"queue_publish_kwargs": {randomstring(): randomstring()}}

    message_bytes = randomstring().encode()
    rqp = ResultQueuePublisher(queue_info=queue_info)
    rqp._channel = MagicMock()
    rqp.publish(message_bytes)
    rqp._channel.basic_publish.assert_called()
    args, kwargs = rqp._channel.basic_publish.call_args
    assert kwargs["body"] == message_bytes
    assert set(queue_info["queue_publish_kwargs"].keys()) < set(kwargs.keys())


def test_rqp_creates_pika_basicproperties(randomstring):
    bp_kwds = set(inspect.signature(pika.BasicProperties.__init__).parameters.keys())
    bp_kwds.discard("self")
    props = {bp_kwds.pop(): True for _ in range(random.randint(1, 3))}
    queue_info = {"queue_publish_kwargs": {"properties": props}}
    rqp = ResultQueuePublisher(queue_info=queue_info)
    basicprops = rqp._publish_kwargs["properties"]
    assert all(getattr(basicprops, k) for k in props)
