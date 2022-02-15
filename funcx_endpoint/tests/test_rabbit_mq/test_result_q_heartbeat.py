import logging
import time

import pika
import pytest

from funcx_endpoint.endpoint.rabbit_mq import ResultQueuePublisher

LOG_FORMAT = "%(levelname) -10s %(asctime)s %(name) -20s %(lineno) -5d: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

endpoint_id_1 = "a9aec9a1-ff86-4d6a-a5b8-5bb160746b5c"


def test_heartbeat_ok():
    """Confirm that result_q_publisher does not disconnect when delay
    between messages exceed heartbeat period
    """
    cred = pika.PlainCredentials("guest", "guest")
    service_params = pika.ConnectionParameters(
        host="localhost",
        heartbeat=1,
        blocked_connection_timeout=2,
        port=5672,
        credentials=cred,
    )

    result_pub = ResultQueuePublisher(
        endpoint_id=endpoint_id_1, pika_conn_params=service_params
    )
    result_pub.connect()

    x = result_pub.publish(b"Hello")
    assert x is None
    time.sleep(5)
    x = result_pub.publish(b"Hello")
    assert x is None


def test_fail_after_manual_close():
    """Confirm that result_q_publisher raises an error following a manual conn close"""
    cred = pika.PlainCredentials("guest", "guest")
    service_params = pika.ConnectionParameters(
        host="localhost",
        heartbeat=1,
        blocked_connection_timeout=2,
        port=5672,
        credentials=cred,
    )

    result_pub = ResultQueuePublisher(
        endpoint_id=endpoint_id_1, pika_conn_params=service_params
    )
    result_pub.connect()

    result_pub.publish(b"Hello")
    result_pub.close()
    with pytest.raises(pika.exceptions.ChannelWrongStateError):
        result_pub.publish(b"Hello")


def test_reconnect_after_disconnect():
    """Confirm that result_q_publisher does not disconnect when delay
    between messages exceed heartbeat period
    """
    cred = pika.PlainCredentials("guest", "guest")
    service_params = pika.ConnectionParameters(
        host="localhost",
        heartbeat=1,
        blocked_connection_timeout=2,
        port=5672,
        credentials=cred,
    )

    result_pub = ResultQueuePublisher(
        endpoint_id=endpoint_id_1, pika_conn_params=service_params
    )
    result_pub.connect()

    result_pub.publish(b"Hello")
    result_pub.close()
    result_pub.connect()
    result_pub.publish(b"Hello")
