import pika
import pytest

CONN_PARAMS = pika.URLParameters("amqp://guest:guest@localhost:5672/")


@pytest.fixture(autouse=True, scope="session")
def ensure_result_queue():
    connection = pika.BlockingConnection(CONN_PARAMS)
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
    return
