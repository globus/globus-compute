import os
import uuid

import pika
import pytest


@pytest.fixture(scope="session")
def endpoint_uuid():
    return str(uuid.UUID(int=0))


@pytest.fixture(scope="session")
def default_endpoint_id():
    return str(uuid.UUID(int=1))


@pytest.fixture(scope="session")
def other_endpoint_id():
    return str(uuid.UUID(int=2))


@pytest.fixture(scope="session")
def rabbitmq_conn_url():
    env_var_name = "RABBITMQ_INTEGRATION_TEST_URI"
    rmq_test_uri = os.getenv(env_var_name, "amqp://guest:guest@localhost:5672/")

    try:
        # Die here and now, first thing, with a hopefully-helpful direct fix suggestion
        # if rmq_test_uri is invalid or otherwise "not working."
        pika.BlockingConnection(pika.URLParameters(rmq_test_uri))
    except Exception as exc:
        msg = (
            f"Failed to connect to RabbitMQ via URI: {rmq_test_uri}\n"
            f"  Do you need to export {env_var_name} ?  Typo?"
        )
        raise ValueError(msg) from exc

    return rmq_test_uri


@pytest.fixture()
def pika_conn_params(rabbitmq_conn_url):
    return pika.URLParameters(rabbitmq_conn_url)
