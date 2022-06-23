from __future__ import annotations

import os
import random
import string
import time
import uuid

import globus_sdk
import pika
import pytest
import responses

import funcx
from funcx.sdk.web_client import FuncxWebClient


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
def tod_session_num():
    yield round(time.time()) % 86400


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


@pytest.fixture(scope="session")
def pika_conn_params(rabbitmq_conn_url):
    return pika.URLParameters(rabbitmq_conn_url)


def _flush_results(pika_conn_params):
    """Reminder: not a fixture; regular method"""
    with pika.BlockingConnection(pika_conn_params) as mq_conn:
        with mq_conn.channel() as chan:
            queue_name = "results"
            chan.exchange_declare(
                exchange="results",
                exchange_type="topic",
                durable=True,
            )
            chan.queue_declare(queue=queue_name, durable=True)
            chan.queue_purge(queue=queue_name)


@pytest.fixture
def flush_results(pika_conn_params):
    _flush_results(pika_conn_params)


@pytest.fixture(scope="session", autouse=True)
def clear_results_queue(pika_conn_params):
    _flush_results(pika_conn_params)
    yield
    _flush_results(pika_conn_params)


@pytest.fixture
def create_result_queue_info(rabbitmq_conn_url, tod_session_num, request):
    def _do_it(connection_url=None, queue_id=None) -> dict:
        exchange_name = "results"
        if not queue_id:
            queue_id = f"test_result_queue_{tod_session_num}__{request.node.name}"
        if not connection_url:
            connection_url = rabbitmq_conn_url
        queue_name = f"{queue_id}.results"
        return {
            "connection_url": connection_url,
            "exchange": exchange_name,
            "queue": queue_name,
            "queue_publish_kwargs": {
                "exchange": exchange_name,
                "routing_key": queue_name,
                "mandatory": True,
                "properties": {
                    "delivery_mode": pika.spec.PERSISTENT_DELIVERY_MODE,
                },
            },
            "test_routing_key": queue_id,
        }

    return _do_it


@pytest.fixture
def result_queue_info(create_result_queue_info) -> dict:
    return create_result_queue_info()


@pytest.fixture
def task_queue_info(rabbitmq_conn_url, tod_session_num, request) -> dict:
    queue_id = f"test_task_queue_{tod_session_num}__{request.node.name}"
    return {
        "connection_url": rabbitmq_conn_url,
        "exchange": "tasks",
        "queue": f"{queue_id}.tasks",
        "test_routing_key": queue_id,
    }


class FakeLoginManager:
    def ensure_logged_in(self) -> None:
        ...

    def logout(self) -> None:
        ...

    def get_auth_client(self) -> globus_sdk.AuthClient:
        return globus_sdk.AuthClient(authorizer=globus_sdk.NullAuthorizer())

    def get_search_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient(authorizer=globus_sdk.NullAuthorizer())

    def get_funcx_web_client(self, *, base_url: str | None = None) -> FuncxWebClient:
        return FuncxWebClient(
            base_url="https://api2.funcx.org/v2/",
            authorizer=globus_sdk.NullAuthorizer(),
        )


@pytest.fixture
def get_standard_funcx_client():
    responses.add(
        method=responses.GET,
        url="https://api2.funcx.org/v2/version",
        headers={"Content-Type": "application/json"},
        json={"api": "0.4.0", "min_ep_version": "0.0.0", "min_sdk_version": "0.0.0"},
    )

    def func():
        return funcx.FuncXClient(
            use_offprocess_checker=False,
            login_manager=FakeLoginManager(),
            do_version_check=False,
        )

    return func


@pytest.fixture
def randomstring():
    def func(length=5, alphabet=string.ascii_letters):
        return "".join(random.choice(alphabet) for _ in range(length))

    return func
