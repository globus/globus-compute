import pika
import pytest

G_CONN_PARAMS = pika.URLParameters("amqp://guest:guest@localhost:5672/")
ENDPOINT_UUID = "9d8f5fcc-8d70-4d8e-a027-510ed6084b2e"


@pytest.fixture()
def conn_params():
    return G_CONN_PARAMS


@pytest.fixture()
def endpoint_uuid():
    return ENDPOINT_UUID


@pytest.fixture(scope="session")
def ensure_result_queue():
    connection = pika.BlockingConnection(G_CONN_PARAMS)
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
