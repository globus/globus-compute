import json
import logging
import multiprocessing
import uuid

import pika
import pika.exceptions
import pytest


def publish_messages(result_pub, count) -> None:
    for i in range(count):
        message = {
            "task_id": str(uuid.uuid4()),
            "result": f"foo-{i}",
        }
        b_message = json.dumps(message).encode()
        result_pub.publish(b_message)

    logging.warning(f"Published {count} messages")


def test_result_queue_basic(start_result_q_publisher):
    """Test that any endpoint can publish
    TO DO: Add in auth to ensure that you can publish results from *any* EP
    Testing purging queue
    """
    result_pub = start_result_q_publisher()
    publish_messages(result_pub, 10)
    result_pub._channel.queue_purge("results")
    result_pub.close()


@pytest.mark.parametrize("size", [10, 2**10, 2**20, (2**20) * 10])
def test_message_integrity_across_sizes(
    size, start_result_q_publisher, start_result_q_subscriber, default_endpoint_id
):
    """Publish count messages from endpoint_1
    Confirm that the subscriber gets all of them.
    """
    result_pub = start_result_q_publisher()
    data = "x" * size
    message = {"task_id": str(uuid.uuid4()), "result": data}
    b_message = json.dumps(message).encode()
    result_pub.publish(b_message)

    results_q = multiprocessing.Queue()
    start_result_q_subscriber(queue=results_q)

    result_message = results_q.get(timeout=2)
    assert result_message == (result_pub.queue_info["test_routing_key"], b_message)


def test_publish_multiple_then_subscribe(
    start_result_q_publisher,
    start_result_q_subscriber,
    default_endpoint_id,
    other_endpoint_id,
    create_result_queue_info,
):
    """Publish count messages from endpoint_1 and endpoint_1
    Confirm that the subscriber gets all of them.
    """
    q_info1 = create_result_queue_info(queue_id=default_endpoint_id)
    q_info2 = create_result_queue_info(queue_id=other_endpoint_id)
    result_pub1 = start_result_q_publisher(override_params=q_info1)
    result_pub2 = start_result_q_publisher(override_params=q_info2)
    total_messages = 20
    publish_messages(result_pub1, count=10)
    publish_messages(result_pub2, count=10)

    results_q = multiprocessing.Queue()
    start_result_q_subscriber(queue=results_q)

    all_results = {}
    for _i in range(total_messages):
        (routing_key, b_message) = results_q.get(timeout=2)
        all_results[routing_key] = all_results.get(routing_key, 0) + 1

    routing_keys_stripped = [key.split(".")[0] for key in all_results]
    assert default_endpoint_id in routing_keys_stripped
    assert other_endpoint_id in routing_keys_stripped
    assert list(all_results.values()) == [10, 10]


@pytest.mark.parametrize(
    "conn_url",
    [
        "amqp://guest:guest@localhost:5670",  # bad port
        "amqp://localhost:5672",  # no credentials
        "amqp://bad:creds@localhost:5672",
    ],
)
def test_broken_connection(
    start_result_q_publisher, create_result_queue_info, conn_url, rabbitmq_conn_url
):
    """Test exception raised on connect with bad connection info"""
    vhost_path = rabbitmq_conn_url.rsplit(":", maxsplit=1)[-1].lstrip("0123456789")
    conn_url += vhost_path
    pika_params = pika.URLParameters(conn_url)
    try:
        pika.BlockingConnection(pika_params)

        # matches known-good credentials, so this won't be an effective test
        # (parameter).  This might happen on the default CI infrastructure for
        # amqp://localhost:5672, which Pika politely translates to
        # amqp://guest:guest@localhost:5672.
        # See: https://pika.readthedocs.io/en/stable/examples/using_urlparameters.html
        return
    except pika.exceptions.AMQPConnectionError:
        # expected path for a valid test
        pass

    q_info = create_result_queue_info(connection_url=conn_url)

    with pytest.raises(pika.exceptions.AMQPConnectionError):
        start_result_q_publisher(override_params=q_info)


def test_disconnect_from_client_side(
    start_result_q_publisher, start_result_q_subscriber
):
    """Confirm that an exception is raised when the connection is closed
    Ideally we use rabbitmqadmin to close the connection, but that is less reliable here
    since the test env may not be have the util, and
    """

    result_pub = start_result_q_publisher()
    res = result_pub.publish(b"Hello test_disconnect_from_client_side 1")
    assert res is None

    result_pub.close()

    with pytest.raises(pika.exceptions.ChannelWrongStateError):
        result_pub.publish(b"Hello test_disconnect_from_client_side 2")
