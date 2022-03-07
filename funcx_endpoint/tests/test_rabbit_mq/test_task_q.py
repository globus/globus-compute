import json
import logging
import multiprocessing
import random
import time
import uuid

from funcx.serialize import FuncXSerializer
from funcx_endpoint.endpoint.rabbit_mq import TaskQueuePublisher, TaskQueueSubscriber

ENDPOINT_ID = "task-q-tests"


def start_task_q_publisher(conn_params):
    task_q = TaskQueuePublisher(endpoint_uuid=ENDPOINT_ID, pika_conn_params=conn_params)
    task_q.connect()
    return task_q


def start_task_q_subscriber(
    out_queue: multiprocessing.Queue,
    disconnect_event: multiprocessing.Event,
    conn_params,
):
    task_q = TaskQueueSubscriber(
        conn_params,
        external_queue=out_queue,
        kill_event=disconnect_event,
        endpoint_uuid=ENDPOINT_ID,
    )
    task_q.start()
    return task_q


def test_synch(conn_params, count=10):

    """Open publisher, and publish to task_q, then open subscriber a fetch"""
    fxs = FuncXSerializer()

    task_q_pub = start_task_q_publisher(conn_params)
    task_q_pub._channel.queue_purge(task_q_pub.queue_name)  # Make sure queue is empty
    messages = {}
    for i in range(count):
        data = list(range(10))
        message = {
            "task_id": str(uuid.uuid4()),
            "result": fxs.serialize(data),
        }
        b_message = json.dumps(message, ensure_ascii=True).encode("utf-8")
        task_q_pub.publish(b_message)
        messages[i] = b_message

    task_q_pub.close()
    logging.warning(f"Published {count} messages, closing task_q_pub")
    logging.warning("Starting task_q_subscriber")
    tasks_out = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()

    proc = start_task_q_subscriber(tasks_out, disconnect_event, conn_params)
    for i in range(count):
        message = tasks_out.get()
        assert messages[i] == message

    proc.terminate()
    return


def fallible_callback(queue: multiprocessing.Queue, message: bytes):
    logging.warning("In callback")
    x = random.randint(1, 10)
    logging.warning(f"{x} > 7")
    if x >= 7:
        raise ValueError
    else:
        logging.warning(f"Got message: {message}")
        queue.put(message)
    return


def test_subscriber_recovery(conn_params):
    """Subscriber terminates after 10 messages, and reconnects."""
    fxs = FuncXSerializer()
    task_q_pub = start_task_q_publisher(conn_params)
    task_q_pub._channel.queue_purge(task_q_pub.queue_name)  # Make sure queue is empty

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
    proc = start_task_q_subscriber(tasks_out, disconnect_event, conn_params)
    logging.warning("Proc started")
    for i in range(10):
        message = tasks_out.get()
        logging.warning(f"Got message: {message}")
        assert messages[i] == message

    # Terminate the connection
    proc.terminate()
    logging.warning("Disconnected")

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
    proc = start_task_q_subscriber(tasks_out, disconnect_event, conn_params)
    logging.warning("Proc started")
    for i in range(10):
        message = tasks_out.get()
        logging.warning(f"Got message: {message}")
        assert messages[i] == message

    proc.terminate()
    task_q_pub.close()
    return


def test_exclusive_subscriber(conn_params):
    """2 subscribers connect, only last one should get any messages"""
    fxs = FuncXSerializer()
    task_q_pub = start_task_q_publisher(conn_params)
    task_q_pub._channel.queue_purge(task_q_pub.queue_name)  # Make sure queue is empty

    # Start two subscribers to the same queue
    tasks_out_1, tasks_out_2 = multiprocessing.Queue(), multiprocessing.Queue()
    disconnect_event_1, disconnect_event_2 = (
        multiprocessing.Event(),
        multiprocessing.Event(),
    )
    proc1 = start_task_q_subscriber(tasks_out_1, disconnect_event_1, conn_params)
    time.sleep(1)
    proc2 = start_task_q_subscriber(tasks_out_2, disconnect_event_2, conn_params)

    logging.warning("TEST: Launching messages")
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
    logging.warning("TEST: Launching messages")

    # Give some delay
    time.sleep(1)

    # Check that the second subscriber did not receive any messages
    assert tasks_out_2.empty()

    # Confirm that the first subscriber received all the messages
    for i in range(10):
        message = tasks_out_1.get(timeout=5)
        logging.warning(f"Got message: {message}")
        assert messages[i] == message

    proc1.terminate()
    proc2.terminate()

    task_q_pub.close()
    return


def test_combined_pub_sub_latency(conn_params, count=10):
    """Confirm that messages published are received."""
    task_q_pub = start_task_q_publisher(conn_params)
    task_q_pub._channel.queue_purge(task_q_pub.queue_name)  # Make sure queue is empty

    tasks_out = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()
    proc = start_task_q_subscriber(tasks_out, disconnect_event, conn_params)

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
    logging.warning(
        f"Message latencies in milliseconds, min:{1000*min(latency):.2f}, "
        f"max:{1000*max(latency):.2f}, avg:{1000*avg_latency:.2f}"
    )

    task_q_pub.close()
    proc.terminate()


def test_combined_throughput(conn_params, count=1000):
    """Confirm that messages published are received."""
    task_q_pub = start_task_q_publisher(conn_params)
    task_q_pub._channel.queue_purge(task_q_pub.queue_name)  # Make sure queue is empty

    tasks_out = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()
    proc = start_task_q_subscriber(tasks_out, disconnect_event, conn_params)

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
        logging.warning(
            f"TTC throughput for {count} messages at {size}B = "
            f"{count/tput_at_size[size]['ttc']:.2f}messages/s"
        )

    task_q_pub.close()
    proc.terminate()
