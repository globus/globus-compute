import json
import logging
import multiprocessing
import uuid

import pika
import pytest

from funcx.serialize import FuncXSerializer
from funcx_endpoint.endpoint.rabbit_mq import (
    ResultQueuePublisher,
    ResultQueueSubscriber,
)

LOG_FORMAT = "%(levelname) -10s %(asctime)s %(name) -20s %(lineno) -5d: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


endpoint_id_1 = "a9aec9a1-ff86-4d6a-a5b8-5bb160746b5c"
endpoint_id_2 = "9b2dbe0f-0420-4256-9f89-71cb8bfb26d2"

CONN_PARAMS = pika.URLParameters("amqp://guest:guest@localhost:5672/")


def start_result_q_publisher(endpoint_id):
    result_pub = ResultQueuePublisher(
        endpoint_id=endpoint_id, pika_conn_params=CONN_PARAMS
    )
    result_pub.connect()
    return result_pub


def start_result_q_subscriber(queue: multiprocessing.Queue) -> ResultQueueSubscriber:

    result_q = ResultQueueSubscriber(
        pika_conn_params=CONN_PARAMS,
        external_queue=queue,
    )
    result_q.start()
    return result_q


def publish_messages(count: int, endpoint_id: str, size=10) -> ResultQueuePublisher:
    fxs = FuncXSerializer()
    result_pub = start_result_q_publisher(endpoint_id)
    for _i in range(count):
        data = bytes(size)
        message = {
            "task_id": str(uuid.uuid4()),
            "result": fxs.serialize(data),
        }
        b_message = json.dumps(message, ensure_ascii=True).encode("utf-8")
        result_pub.publish(b_message)

    logger.warning(f"Published {count} messages")
    return result_pub


def test_result_queue_basic(count=10):
    """Test that any endpoint can publish
    TO DO: Add in auth to ensure that you can publish results from *any* EP
    Testing purging queue
    """
    result_pub = publish_messages(count=count, endpoint_id=str(uuid.uuid4()))
    result_pub._channel.queue_purge("results")
    result_pub.close()


@pytest.mark.parametrize("size", [10, 2 ** 10, 2 ** 20, (2 ** 20) * 10])
def test_message_integrity_across_sizes(size):
    """Publish count messages from endpoint_1 and endpoint_1
    Confirm that the subscriber gets all of them.
    """

    fxs = FuncXSerializer()
    result_pub = start_result_q_publisher(endpoint_id_1)
    data = bytes(size)
    message = {
        "task_id": str(uuid.uuid4()),
        "result": fxs.serialize(data),
    }
    b_message = json.dumps(message, ensure_ascii=True).encode("utf-8")
    result_pub.publish(b_message)

    results_q = multiprocessing.Queue()
    sub_proc = start_result_q_subscriber(results_q)

    (routing_key, received_message) = results_q.get(timeout=2)
    assert endpoint_id_1 in routing_key
    assert received_message == b_message
    sub_proc.terminate()


def test_publish_multiple_then_subscribe(count=10):
    """Publish count messages from endpoint_1 and endpoint_1
    Confirm that the subscriber gets all of them.
    """
    total_messages = 20
    publish_messages(count=count, endpoint_id=endpoint_id_1)
    publish_messages(count=count, endpoint_id=endpoint_id_2)

    results_q = multiprocessing.Queue()
    sub_proc = start_result_q_subscriber(results_q)

    all_results = {}
    for _i in range(total_messages):
        (routing_key, b_message) = results_q.get(timeout=2)
        all_results[routing_key] = all_results.get(routing_key, 0) + 1

    routing_keys_stripped = [key.split(".")[0] for key in all_results]
    assert endpoint_id_1 in routing_keys_stripped
    assert endpoint_id_2 in routing_keys_stripped
    assert list(all_results.values()) == [10, 10]

    sub_proc.terminate()


def test_broken_connection():
    """Test exception raised on connect with bad connection info"""
    cred = pika.PlainCredentials("guest", "guest")
    service_params = pika.ConnectionParameters(
        host="localhost", heartbeat=60, port=5671, credentials=cred  # Wrong port
    )

    result_pub = ResultQueuePublisher(
        endpoint_id=endpoint_id_1, pika_conn_params=service_params
    )
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        result_pub.connect()


def test_disconnect_from_client_side():
    """Confirm that an exception is raised when the connection is closed
    Ideally we use rabbitmqadmin to close the connection, but that is less reliable here
    since the test env may not be have the util, and
    """
    cred = pika.PlainCredentials("guest", "guest")
    service_params = pika.ConnectionParameters(
        host="localhost", heartbeat=60, port=5672, credentials=cred
    )

    result_pub = ResultQueuePublisher(
        endpoint_id=endpoint_id_1, pika_conn_params=service_params
    )
    result_pub.connect()
    res = result_pub.publish(b"Hello")
    assert res is None

    result_pub.close()

    with pytest.raises(pika.exceptions.ChannelWrongStateError):
        result_pub.publish(b"Hello")
