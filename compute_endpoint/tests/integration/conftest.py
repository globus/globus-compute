from __future__ import annotations

import os
import queue
import random
import string
import threading

import pika
import pika.exceptions
import pytest
from globus_compute_endpoint.endpoint.rabbit_mq import (
    ResultPublisher,
    TaskQueueSubscriber,
)
from pika.exchange_type import ExchangeType
from tests.integration.test_rabbit_mq.result_queue_subscriber import (
    ResultQueueSubscriber,
)
from tests.integration.test_rabbit_mq.task_queue_publisher import TaskQueuePublisher
from tests.utils import try_assert


@pytest.fixture(scope="session")
def rabbitmq_conn_url():
    env_var_name = "RABBITMQ_INTEGRATION_TEST_URI"
    rmq_test_uri = os.getenv(env_var_name, "amqp:///")

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
        routing_key = f"{queue_id}.results"
        return {
            "connection_url": connection_url,
            "exchange": exchange_name,
            "queue": "results",
            "queue_publish_kwargs": {
                "exchange": exchange_name,
                "routing_key": routing_key,
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
def create_heartbeat_queue_info(rabbitmq_conn_url, tod_session_num, request):
    def _do_it(connection_url=None, queue_id=None) -> dict:
        exchange_name = "heartbeats"
        if not queue_id:
            queue_id = f"test_heartbeat_queue_{tod_session_num}__{request.node.name}"
        if not connection_url:
            connection_url = rabbitmq_conn_url
        routing_key = f"{queue_id}.heartbeats"
        return {
            "connection_url": connection_url,
            "exchange": exchange_name,
            "queue": exchange_name,
            "queue_publish_kwargs": {
                "exchange": exchange_name,
                "routing_key": routing_key,
                "mandatory": True,
                "properties": {
                    "delivery_mode": pika.spec.PERSISTENT_DELIVERY_MODE,
                },
            },
            "test_routing_key": queue_id,
        }

    return _do_it


@pytest.fixture
def heartbeat_queue_info(create_heartbeat_queue_info) -> dict:
    return create_heartbeat_queue_info()


@pytest.fixture
def task_queue_info(rabbitmq_conn_url, tod_session_num, request) -> dict:
    queue_id = f"test_task_queue_{tod_session_num}__{request.node.name}"
    return {
        "connection_url": rabbitmq_conn_url,
        "exchange": "tasks",
        "queue": f"{queue_id}.tasks",
        "test_routing_key": queue_id,
    }


@pytest.fixture(scope="session")
def ensure_result_queue(pika_conn_params):
    queues_created = []

    def _do_ensure(exchange_opts=None, queue_opts=None):
        if not exchange_opts:
            exchange_opts = {
                "exchange": "results",
                "exchange_type": ExchangeType.topic.value,
                "durable": True,
            }
        if not queue_opts:
            queue_opts = {"queue": "results", "durable": True}
            routing_key = "*.results"
        else:
            routing_key = queue_opts["queue"]

        with pika.BlockingConnection(pika_conn_params) as mq_conn:
            with mq_conn.channel() as chan:
                chan.exchange_declare(**exchange_opts)
                chan.queue_declare(**queue_opts)
                queues_created.append(queue_opts["queue"])
                chan.queue_bind(
                    queue=queue_opts["queue"],
                    exchange=exchange_opts["exchange"],
                    routing_key=routing_key,
                )

    _do_ensure()  # The main "results" should always exist for our tests
    yield _do_ensure

    with pika.BlockingConnection(pika_conn_params) as mq_conn:
        with mq_conn.channel() as chan:
            for q_name in queues_created:
                chan.queue_delete(q_name)


@pytest.fixture(scope="session")
def ensure_heartbeat_queue(pika_conn_params):
    queues_created = []

    def _do_ensure(exchange_opts=None, queue_opts=None):
        if not exchange_opts:
            exchange_opts = {
                "exchange": "heartbeats",
                "exchange_type": ExchangeType.topic.value,
                "durable": True,
            }
        if not queue_opts:
            queue_opts = {"queue": "heartbeats", "durable": True}
            routing_key = "*.heartbeats"
        else:
            routing_key = queue_opts["queue"]

        with pika.BlockingConnection(pika_conn_params) as mq_conn:
            with mq_conn.channel() as chan:
                chan.exchange_declare(**exchange_opts)
                chan.queue_declare(**queue_opts)
                queues_created.append(queue_opts["queue"])
                chan.queue_bind(
                    queue=queue_opts["queue"],
                    exchange=exchange_opts["exchange"],
                    routing_key=routing_key,
                )

    _do_ensure()  # The main "heartbeats" should always exist for our tests
    yield _do_ensure

    with pika.BlockingConnection(pika_conn_params) as mq_conn:
        with mq_conn.channel() as chan:
            for q_name in queues_created:
                chan.queue_delete(q_name)


@pytest.fixture
def start_task_q_subscriber(
    task_queue_info,
    ensure_task_queue,
):
    qs_list: list[TaskQueueSubscriber] = []

    def func(
        *,
        task_queue: queue.SimpleQueue | None = None,
        override_params: pika.connection.Parameters | None = None,
    ):
        if task_queue is None:
            task_queue = queue.SimpleQueue()
        q_info = task_queue_info if override_params is None else override_params
        ensure_task_queue(queue_opts={"queue": q_info["queue"]})

        qs = TaskQueueSubscriber(queue_info=q_info, pending_task_queue=task_queue)
        qs.start()
        qs_list.append(qs)
        return qs

    yield func

    while qs_list:
        qs = qs_list.pop()
        qs.stop()
        qs.join()


@pytest.fixture
def start_result_q_subscriber(pika_conn_params):
    qs_list: list[ResultQueueSubscriber] = []

    def func(
        *,
        result_q: queue.SimpleQueue | None = None,
        kill_event: threading.Event | None = None,
        override_params: pika.connection.Parameters | None = None,
    ):
        if kill_event is None:
            kill_event = threading.Event()
        if result_q is None:
            result_q = queue.SimpleQueue()
        qs = ResultQueueSubscriber(
            conn_params=pika_conn_params if not override_params else override_params,
            external_queue=result_q,
            kill_event=kill_event,
        )
        qs.start()
        qs_list.append(qs)
        if not qs.test_class_ready.wait(10):
            raise AssertionError("Result Queue subscriber failed to initialize")
        return qs

    yield func

    while qs_list:
        qs_list.pop().stop()


@pytest.fixture
def start_result_q_publisher(
    result_queue_info,
    ensure_result_queue,
):
    qp_list: list[ResultPublisher] = []

    def func(
        *,
        override_params: dict | None = None,
        queue_purge: bool = True,
    ):
        q_info = result_queue_info if override_params is None else override_params
        exchange_name, queue_name = q_info.get("exchange"), q_info.get("queue")
        if exchange_name:  # We'll call it an error to specify only one
            exchange_opts = {
                "exchange": exchange_name,
                "exchange_type": ExchangeType.topic.value,
                "durable": True,
            }
            queue_opts = {"queue": queue_name, "durable": True}
            ensure_result_queue(exchange_opts=exchange_opts, queue_opts=queue_opts)

        qp = ResultPublisher(queue_info=q_info)
        qp.start()
        qp_list.append(qp)
        if queue_purge:  # Make sure queue is empty
            try_assert(lambda: qp._mq_chan is not None)
            qp._mq_chan.queue_purge(q_info["queue"])
        return qp

    yield func

    while qp_list:
        qp_list.pop().stop(timeout=None)


@pytest.fixture
def start_heartbeat_q_publisher(heartbeat_queue_info, ensure_heartbeat_queue):
    qp_list: list[ResultPublisher] = []

    def func(
        *,
        override_params: dict | None = None,
        queue_purge: bool = True,
    ):
        q_info = heartbeat_queue_info if override_params is None else override_params
        exchange_name, queue_name = q_info.get("exchange"), q_info.get("queue")
        if exchange_name:  # We'll call it an error to specify only one
            exchange_opts = {
                "exchange": exchange_name,
                "exchange_type": ExchangeType.topic.value,
                "durable": True,
            }
            queue_opts = {"queue": queue_name, "durable": True}
            ensure_heartbeat_queue(exchange_opts=exchange_opts, queue_opts=queue_opts)

        qp = ResultPublisher(queue_info=q_info)
        qp.start()
        qp_list.append(qp)
        if queue_purge:  # Make sure queue is empty
            try_assert(lambda: qp._mq_chan is not None)
            qp._mq_chan.queue_purge(q_info["queue"])
        return qp

    yield func

    while qp_list:
        qp_list.pop().stop(timeout=None)


@pytest.fixture
def start_task_q_publisher(
    task_queue_info,
    ensure_task_queue,
    default_endpoint_id,
):
    qp_list: list[TaskQueuePublisher] = []

    def func(
        *,
        override_params: pika.connection.Parameters | None = None,
        queue_purge: bool = True,
    ):
        q_info = task_queue_info if override_params is None else override_params
        exchange_name, queue_name = q_info.get("exchange"), q_info.get("queue")
        if exchange_name:  # We'll call it an error to specify only one
            exchange_opts = {
                "exchange": exchange_name,
                "exchange_type": ExchangeType.direct.value,
            }
            queue_opts = {"queue": queue_name, "arguments": {"x-expires": 30 * 1000}}
            ensure_task_queue(exchange_opts=exchange_opts, queue_opts=queue_opts)

        qp = TaskQueuePublisher(queue_info=q_info)
        qp.connect()
        qp_list.append(qp)
        if queue_purge:  # Make sure queue is empty
            qp._channel.queue_purge(q_info["queue"])
        return qp

    yield func

    while qp_list:
        qp_list.pop().close()


@pytest.fixture(scope="session")
def ensure_task_queue(pika_conn_params, tod_session_num, request):
    queues_created = []

    def _do_ensure(exchange_opts=None, queue_opts=None):
        if not exchange_opts:
            exchange_opts = {
                "exchange": "tasks",
                "exchange_type": ExchangeType.direct.value,
            }
        exchange_opts.setdefault("durable", True)
        if not queue_opts:
            rndm = "".join(random.choice(string.ascii_letters) for _ in range(5))
            queue_id = f"queue_{tod_session_num}__{request.node.name}__{rndm}"
            queue_opts = {"queue": f"task_{queue_id}.tasks"}
        # play nice with dev/test resources; auto clean
        queue_opts.setdefault("arguments", {"x-expires": 30 * 1000})

        with pika.BlockingConnection(pika_conn_params) as mq_conn:
            with mq_conn.channel() as chan:
                chan.exchange_declare(**exchange_opts)
                chan.queue_declare(**queue_opts)
                queues_created.append(queue_opts["queue"])
                chan.queue_bind(
                    queue=queue_opts["queue"],
                    exchange=exchange_opts["exchange"],
                    routing_key=queue_opts["queue"],
                )

    yield _do_ensure

    with pika.BlockingConnection(pika_conn_params) as mq_conn:
        with mq_conn.channel() as chan:
            for q_name in queues_created:
                chan.queue_delete(q_name)
