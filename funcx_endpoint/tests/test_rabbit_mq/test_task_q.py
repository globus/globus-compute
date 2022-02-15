import json
import logging
import multiprocessing
import random
import time
import uuid

import pika
import pytest

from funcx.serialize import FuncXSerializer
from funcx_endpoint.endpoint.rabbit_mq import TaskQueuePublisher, TaskQueueSubscriber

LOG_FORMAT = "%(levelname) -10s %(asctime)s %(name) -20s %(lineno) -5d: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


@pytest.fixture()
def offprocess_consumer():
    print("Starting offprocess_consumer")
    yield True
    print("Closing offprocess_consumer")


def listen_for_tasks():
    cred = pika.PlainCredentials("guest", "guest")
    service_params = pika.ConnectionParameters(
        host="localhost", heartbeat=60, port=5672, credentials=cred
    )

    endpoint_id = "231fab4e-630a-4d76-bbbb-cf0b4aedbdf9"

    task_queue = TaskQueueSubscriber(service_params, endpoint_name=endpoint_id)
    task_queue.run()


def start_task_q_publisher():
    cred = pika.PlainCredentials("guest", "guest")
    service_params = pika.ConnectionParameters(
        host="localhost", heartbeat=60, port=5672, credentials=cred
    )

    endpoint_id = "231fab4e-630a-4d76-bbbb-cf0b4aedbdf9"

    task_q = TaskQueuePublisher(
        endpoint_name=endpoint_id, pika_conn_params=service_params
    )
    task_q.connect()
    return task_q


def start_task_q_subscriber(
    out_queue: multiprocessing.Queue, disconnect_event: multiprocessing.Event
):
    cred = pika.PlainCredentials("guest", "guest")
    service_params = pika.ConnectionParameters(
        host="localhost", heartbeat=60, port=5672, credentials=cred
    )

    endpoint_id = "231fab4e-630a-4d76-bbbb-cf0b4aedbdf9"

    task_q = TaskQueueSubscriber(
        endpoint_name=endpoint_id, pika_conn_params=service_params
    )
    task_q.connect()
    task_q.run(queue=out_queue, disconnect_event=disconnect_event)
    return task_q


def test_synch():
    """Open publisher, and publish to task_q, then open subscriber a fetch"""
    fxs = FuncXSerializer()
    task_q_pub = start_task_q_publisher()
    task_q_pub.queue_purge()  # Make sure queue is empty
    messages = {}
    for i in range(10):
        data = list(range(10))
        message = {
            "task_id": str(uuid.uuid4()),
            "result": fxs.serialize(data),
        }
        b_message = json.dumps(message, ensure_ascii=True).encode("utf-8")
        task_q_pub.publish(b_message)
        messages[i] = b_message

    task_q_pub.close()

    tasks_out = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()

    proc = multiprocessing.Process(
        target=start_task_q_subscriber,
        args=(
            tasks_out,
            disconnect_event,
        ),
    )
    proc.start()
    for i in range(10):
        message = tasks_out.get()
        logger.warning(f"Got message: {message}")
        assert messages[i] == message

    proc.terminate()
    return


def fallible_callback(queue: multiprocessing.Queue, message: bytes):
    logger.warning("In callback")
    x = random.randint(1, 10)
    logger.warning(f"{x} > 7")
    if x >= 7:
        raise ValueError
    else:
        logger.warning(f"Got message: {message}")
        queue.put(message)
    return


def test_subscriber_recovery():
    """Subscriber terminates after 10 messages, and reconnects."""
    fxs = FuncXSerializer()
    task_q_pub = start_task_q_publisher()
    task_q_pub.queue_purge()  # Make sure queue is empty

    # Launch 10 messages
    messages = {}
    for i in range(10):
        data = list(range(10))
        message = {
            "task_id": str(uuid.uuid4()),
            "result": fxs.serialize(data),
        }
        b_message = json.dumps(message, ensure_ascii=True).encode("utf-8")
        task_q_pub.publish(b_message)
        messages[i] = b_message

    tasks_out = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()

    # Listen for 10 messages
    proc = multiprocessing.Process(
        target=start_task_q_subscriber,
        args=(
            tasks_out,
            disconnect_event,
        ),
    )
    proc.start()
    logger.warning("Proc started")
    for i in range(10):
        message = tasks_out.get()
        logger.warning(f"Got message: {message}")
        assert messages[i] == message

    # Terminate the connection
    proc.terminate()

    # Launch 10 messages
    messages = {}
    for i in range(10):
        data = list(range(10))
        message = {
            "task_id": str(uuid.uuid4()),
            "result": fxs.serialize(data),
        }
        b_message = json.dumps(message, ensure_ascii=True).encode("utf-8")
        task_q_pub.publish(b_message)
        messages[i] = b_message

    # Listen for the messages on a new connection
    proc = multiprocessing.Process(
        target=start_task_q_subscriber,
        args=(
            tasks_out,
            disconnect_event,
        ),
    )
    proc.start()
    logger.warning("Proc started")
    for i in range(10):
        message = tasks_out.get()
        logger.warning(f"Got message: {message}")
        assert messages[i] == message

    proc.terminate()

    task_q_pub.close()
    return


def test_exclusive_subscriber():
    """2 subscribers connect, only last one should get any messages"""
    fxs = FuncXSerializer()
    task_q_pub = start_task_q_publisher()
    task_q_pub.queue_purge()  # Make sure queue is empty

    # Start two subscribers to the same queue
    tasks_out_1, tasks_out_2 = multiprocessing.Queue(), multiprocessing.Queue()
    disconnect_event_1, disconnect_event_2 = (
        multiprocessing.Event(),
        multiprocessing.Event(),
    )
    proc1 = multiprocessing.Process(
        target=start_task_q_subscriber,
        args=(
            tasks_out_1,
            disconnect_event_1,
        ),
    )
    proc2 = multiprocessing.Process(
        target=start_task_q_subscriber,
        args=(
            tasks_out_2,
            disconnect_event_2,
        ),
    )
    proc1.start()
    time.sleep(1)
    # TO-DO figure out from stephen the caplog mechanism to confirm that
    # a warning/error is logged.
    proc2.start()

    logger.warning("TEST: Launching messages")
    # Launch 10 messages
    messages = {}
    for i in range(10):
        data = list(range(10))
        message = {
            "task_id": str(uuid.uuid4()),
            "result": fxs.serialize(data),
        }
        b_message = json.dumps(message, ensure_ascii=True).encode("utf-8")
        task_q_pub.publish(b_message)
        messages[i] = b_message
    logger.warning("TEST: Launching messages")

    # Give some delay
    time.sleep(1)

    # Check that the second subscriber did not receive any messages
    assert tasks_out_2.empty()

    # Confirm that the first subscriber received all the messages
    for i in range(10):
        message = tasks_out_1.get(timeout=5)
        logger.warning(f"Got message: {message}")
        assert messages[i] == message

    proc1.terminate()
    proc2.terminate()

    task_q_pub.close()
    return


def test_combined_pub_sub_latency(count=10):
    """Confirm that messages published are received."""
    task_q_pub = start_task_q_publisher()
    task_q_pub.queue_purge()  # Make sure queue is empty

    tasks_out = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()
    proc = multiprocessing.Process(
        target=start_task_q_subscriber,
        args=(
            tasks_out,
            disconnect_event,
        ),
    )
    proc.start()

    latency = []
    for i in range(count):
        b_message = f"Hello World! {i}".encode()
        start_t = time.time()
        task_q_pub.publish(b_message)
        x = tasks_out.get()
        delta = time.time() - start_t
        latency.append(delta)
        assert b_message == x

    avg_latency = sum(latency) / len(latency)
    logger.warning(
        f"Message latencies in milliseconds, min:{1000*min(latency):.2f}, "
        f"max:{1000*max(latency):.2f}, avg:{1000*avg_latency:.2f}"
    )

    task_q_pub.close()
    proc.terminate()


def test_combined_throughput(count=1000):
    """Confirm that messages published are received."""
    task_q_pub = start_task_q_publisher()
    task_q_pub.queue_purge()  # Make sure queue is empty

    tasks_out = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()
    proc = multiprocessing.Process(
        target=start_task_q_subscriber,
        args=(
            tasks_out,
            disconnect_event,
        ),
    )
    proc.start()

    tput_at_size = {}
    # Do 10 rounds of throughput measures
    for i in range(10):
        data_size = 2 ** i
        b_message = bytes(data_size)
        start_t = time.time()
        for _i in range(count):
            task_q_pub.publish(b_message)
        send_t = time.time() - start_t
        for _i in range(count):
            message_received = tasks_out.get()
            assert len(message_received) >= data_size
        delta = time.time() - start_t
        tput_at_size[data_size] = {"send": send_t, "ttc": delta}
    for size in tput_at_size:
        logger.warning(
            f"TTC throughput for {count} messages at {size}B = "
            f"{count/tput_at_size[size]['ttc']:.2f}messages/s"
        )

    task_q_pub.close()
    proc.terminate()
