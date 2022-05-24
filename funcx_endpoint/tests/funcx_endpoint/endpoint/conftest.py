import pika
import pytest


@pytest.fixture(scope="session")
def ensure_result_queue(pika_conn_params):
    connection = pika.BlockingConnection(pika_conn_params)
    channel = connection.channel()
    channel.exchange_declare(
        exchange="results",
        exchange_type="topic",
    )
    channel.queue_declare(queue="results")
    channel.queue_bind(
        queue="results",
        exchange="results",
        routing_key="*results",
    )
    channel.close()
    connection.close()
