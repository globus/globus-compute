from __future__ import annotations

import multiprocessing
import uuid

import pika
import pytest

from funcx_endpoint.endpoint.rabbit_mq import (
    RabbitPublisherStatus,
    ResultQueuePublisher,
    ResultQueueSubscriber,
    TaskQueuePublisher,
    TaskQueueSubscriber,
)


@pytest.fixture(scope="session")
def default_endpoint_id():
    return str(uuid.UUID(int=1))


@pytest.fixture(scope="session")
def other_endpoint_id():
    return str(uuid.UUID(int=2))


@pytest.fixture(scope="session")
def rabbitmq_conn_url():
    return "amqp://guest:guest@localhost:5672/"


@pytest.fixture(autouse=True, scope="session")
def ensure_result_queue(rabbitmq_conn_url):
    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_conn_url))
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


@pytest.fixture()
def conn_params(rabbitmq_conn_url):
    return pika.URLParameters(rabbitmq_conn_url)


@pytest.fixture
def running_subscribers(request):
    run_list = []

    def cleanup():
        for x in run_list:
            try:  # cannot check is_alive on closed proc
                is_alive = x.is_alive()
            except ValueError:
                is_alive = False
            if is_alive:
                try:
                    x.stop()
                except Exception as e:
                    x.terminate()
                    raise Exception(
                        f"{x.__class__.__name__} did not shutdown correctly"
                    ) from e

    request.addfinalizer(cleanup)
    return run_list


@pytest.fixture
def start_task_q_subscriber(running_subscribers, conn_params, default_endpoint_id):
    def func(
        *,
        endpoint_id: str | None = None,
        queue: multiprocessing.Queue | None = None,
        kill_event: multiprocessing.Event | None = None,
        override_params: pika.connection.Parameters | None = None,
    ):
        if endpoint_id is None:
            endpoint_id = default_endpoint_id
        if kill_event is None:
            kill_event = multiprocessing.Event()
        if queue is None:
            queue = multiprocessing.Queue()
        task_q = TaskQueueSubscriber(
            conn_params=conn_params if override_params is None else override_params,
            external_queue=queue,
            kill_event=kill_event,
            endpoint_id=endpoint_id,
        )
        task_q.start()
        running_subscribers.append(task_q)
        return task_q

    return func


@pytest.fixture
def start_result_q_subscriber(running_subscribers, conn_params):
    def func(
        *,
        queue: multiprocessing.Queue | None = None,
        kill_event: multiprocessing.Event | None = None,
        override_params: pika.connection.Parameters | None = None,
    ):
        if kill_event is None:
            kill_event = multiprocessing.Event()
        if queue is None:
            queue = multiprocessing.Queue()
        result_q = ResultQueueSubscriber(
            conn_params=conn_params if override_params is None else override_params,
            external_queue=queue,
            kill_event=kill_event,
        )
        result_q.start()
        running_subscribers.append(result_q)
        return result_q

    return func


@pytest.fixture
def running_publishers(request):
    run_list = []

    def cleanup():
        for x in run_list:
            if x.status is RabbitPublisherStatus.connected:
                x.close()

    request.addfinalizer(cleanup)
    return run_list


@pytest.fixture
def start_result_q_publisher(running_publishers, conn_params, default_endpoint_id):
    def func(
        *,
        endpoint_id: str | None = None,
        override_params: pika.connection.Parameters | None = None,
        queue_purge: bool = True,
    ):
        if endpoint_id is None:
            endpoint_id = default_endpoint_id
        result_pub = ResultQueuePublisher(
            endpoint_id=endpoint_id,
            conn_params=conn_params if override_params is None else override_params,
        )
        result_pub.connect()
        if queue_purge:  # Make sure queue is empty
            result_pub._channel.queue_purge("results")
        running_publishers.append(result_pub)
        return result_pub

    return func


@pytest.fixture
def start_task_q_publisher(running_publishers, conn_params, default_endpoint_id):
    def func(
        *,
        endpoint_id: str | None = None,
        override_params: pika.connection.Parameters | None = None,
        queue_purge: bool = True,
    ):
        if endpoint_id is None:
            endpoint_id = default_endpoint_id
        task_pub = TaskQueuePublisher(
            endpoint_id=endpoint_id,
            conn_params=conn_params if override_params is None else override_params,
        )
        task_pub.connect()
        if queue_purge:  # Make sure queue is empty
            task_pub._channel.queue_purge(task_pub.queue_name)
        running_publishers.append(task_pub)
        return task_pub

    return func
