import pika
import pytest

from funcx_endpoint.endpoint.rabbit_mq import ResultQueuePublisher

CONN_PARAMS = pika.URLParameters("amqp://guest:guest@localhost:5672/")


@pytest.fixture(scope="module")
def ensure_result_queue():
    result_pub = ResultQueuePublisher(
        endpoint_id="FIXTURE", pika_conn_params=CONN_PARAMS
    )
    result_pub.connect()
    result_pub._channel.queue_declare(queue="results")
    result_pub._channel.queue_bind(
        queue="results",
        exchange="results",
        routing_key="*results",
    )
    result_pub.close()
    return
