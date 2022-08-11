from __future__ import annotations

import multiprocessing
import random
import string

# multiprocessing.Event is a method, not a class
# to annotate, we need the "real" class
# see: https://github.com/python/typeshed/issues/4266
from multiprocessing.synchronize import Event as EventType

import pika
import pytest
from pika.exchange_type import ExchangeType
from tests.test_rabbit_mq.result_queue_subscriber import ResultQueueSubscriber
from tests.test_rabbit_mq.task_queue_publisher import TaskQueuePublisher

from funcx_endpoint.endpoint.rabbit_mq import (
    RabbitPublisherStatus,
    ResultQueuePublisher,
    TaskQueueSubscriber,
)


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
                    routing_key="*results",
                )

    yield _do_ensure

    with pika.BlockingConnection(pika_conn_params) as mq_conn:
        with mq_conn.channel() as chan:
            for q_name in queues_created:
                chan.queue_delete(q_name)


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
def start_task_q_subscriber(
    running_subscribers,
    task_queue_info,
    default_endpoint_id,
    ensure_task_queue,
):
    def func(
        *,
        endpoint_id: str | None = None,
        queue: multiprocessing.Queue | None = None,
        quiesce_event: EventType | None = None,
        override_params: pika.connection.Parameters | None = None,
    ):
        if endpoint_id is None:
            endpoint_id = default_endpoint_id
        if quiesce_event is None:
            quiesce_event = multiprocessing.Event()
        if queue is None:
            queue = multiprocessing.Queue()
        q_info = task_queue_info if override_params is None else override_params
        ensure_task_queue(queue_opts={"queue": q_info["queue"]})

        task_q = TaskQueueSubscriber(
            queue_info=q_info,
            external_queue=queue,
            quiesce_event=quiesce_event,
            endpoint_id=endpoint_id,
        )
        task_q.start()
        running_subscribers.append(task_q)
        return task_q

    return func


@pytest.fixture
def start_result_q_subscriber(running_subscribers, pika_conn_params):
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
            conn_params=pika_conn_params if not override_params else override_params,
            external_queue=queue,
            kill_event=kill_event,
        )
        result_q.start()
        running_subscribers.append(result_q)
        if not result_q.test_class_ready.wait(10):
            raise AssertionError("Result Queue subscriber failed to initialize")
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
def start_result_q_publisher(
    running_publishers,
    result_queue_info,
    ensure_result_queue,
):
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
            queue_opts = {"queue": queue_name}
            ensure_result_queue(exchange_opts=exchange_opts, queue_opts=queue_opts)

        result_pub = ResultQueuePublisher(queue_info=q_info)
        result_pub.connect()
        if queue_purge:  # Make sure queue is empty
            result_pub._channel.queue_purge(q_info["queue"])
        running_publishers.append(result_pub)
        return result_pub

    return func


@pytest.fixture
def start_task_q_publisher(
    running_publishers,
    task_queue_info,
    ensure_task_queue,
    default_endpoint_id,
):
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

        task_pub = TaskQueuePublisher(queue_info=q_info)
        task_pub.connect()
        if queue_purge:  # Make sure queue is empty
            task_pub._channel.queue_purge(q_info["queue"])
        running_publishers.append(task_pub)
        return task_pub

    return func
