import json
import logging
import multiprocessing
import time
import uuid


def test_synch(start_task_q_publisher, start_task_q_subscriber, count=10):

    """Open publisher, and publish to task_q, then open subscriber a fetch"""
    task_q_pub = start_task_q_publisher()

    messages = []
    for i in range(count):
        message = {"task_id": str(uuid.uuid4()), "result": f"foo-{i}"}
        b_message = json.dumps(message).encode()
        messages.append(b_message)
        task_q_pub.publish(b_message)

    logging.warning(f"Published {count} messages, closing task_q_pub")
    logging.warning("Starting task_q_subscriber")

    tasks_out = multiprocessing.Queue()
    start_task_q_subscriber(queue=tasks_out)
    for i in range(count):
        message = tasks_out.get()
        assert messages[i] == message


def test_subscriber_recovery(start_task_q_publisher, start_task_q_subscriber):
    """Subscriber terminates after 10 messages, and reconnects."""
    task_q_pub = start_task_q_publisher()

    # Launch 10 messages
    messages = []
    for i in range(10):
        message = {"task_id": str(uuid.uuid4()), "result": f"foo-{i}"}
        b_message = json.dumps(message).encode()
        task_q_pub.publish(b_message)
        messages.append(b_message)

    # Listen for 10 messages
    tasks_out = multiprocessing.Queue()
    kill_event = multiprocessing.Event()
    proc = start_task_q_subscriber(queue=tasks_out, kill_event=kill_event)
    logging.warning("Proc started")
    for i in range(10):
        message = tasks_out.get()
        assert messages[i] == message

    # Terminate the connection
    proc.stop()
    logging.warning("Disconnected")

    # Launch 10 more messages
    messages = []
    for i in range(10):
        message = {
            "task_id": str(uuid.uuid4()),
            "result": f"bar-{i}",
        }
        b_message = json.dumps(message).encode()
        task_q_pub.publish(b_message)
        messages.append(b_message)

    # Listen for the messages on a new connection
    kill_event.clear()
    proc = start_task_q_subscriber(queue=tasks_out, kill_event=kill_event)

    logging.warning("Replacement proc started")
    for i in range(10):
        logging.warning("getting message")
        message = tasks_out.get()
        logging.warning(f"Got message: {message}")
        assert messages[i] == message


def test_exclusive_subscriber(start_task_q_publisher, start_task_q_subscriber):
    """2 subscribers connect, only first one should get any messages"""
    task_q_pub = start_task_q_publisher()

    # Start two subscribers on the same rabbit queue
    tasks_out_1, tasks_out_2 = multiprocessing.Queue(), multiprocessing.Queue()
    start_task_q_subscriber(queue=tasks_out_1)
    time.sleep(1)
    start_task_q_subscriber(queue=tasks_out_2)

    logging.warning("TEST: Launching messages")
    # Launch 10 messages
    messages = []
    for i in range(10):
        message = {
            "task_id": str(uuid.uuid4()),
            "result": f"foo={i}",
        }
        b_message = json.dumps(message).encode("utf-8")
        task_q_pub.publish(b_message)
        messages.append(b_message)
    logging.warning("TEST: Launching messages")

    # Confirm that the first subscriber received all the messages
    for i in range(10):
        message = tasks_out_1.get(timeout=1)
        logging.warning(f"Got message: {message}")
        assert messages[i] == message

    # Check that the second subscriber did not receive any messages
    assert tasks_out_2.empty()


def test_perf_combined_pub_sub_latency(start_task_q_publisher, start_task_q_subscriber):
    """Confirm that messages published are received."""
    task_q_pub = start_task_q_publisher()

    tasks_out = multiprocessing.Queue()
    start_task_q_subscriber(queue=tasks_out)

    latency = []
    for i in range(100):
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
    # average latency is expected to be below 5ms
    # if it exceeds this, it means something is wrong
    assert avg_latency < 0.005


def test_perf_combined_pub_sub_throughput(
    start_task_q_publisher, start_task_q_subscriber
):
    """Confirm that messages published are received."""
    task_q_pub = start_task_q_publisher()
    tasks_out = multiprocessing.Queue()
    start_task_q_subscriber(queue=tasks_out)

    num_messages = 1000

    for factor in range(10):
        message_size = 2**factor
        b_message = bytes(message_size)

        start_t = time.time()
        for _i in range(num_messages):
            task_q_pub.publish(b_message)
        send_time = time.time() - start_t

        for _i in range(num_messages):
            tasks_out.get()
        total_time = time.time() - start_t

        sent_per_second = num_messages / send_time
        messages_per_second = num_messages / total_time

        logging.warning(
            f"task throughput for {num_messages} messages at {message_size}B = "
            f"{messages_per_second:.2f} messages/s"
        )
        # each size should record at least 500 messages per second even in an
        # untuned context with other processes running
        # slower than that indicates that a serious performance regression has
        # been introduced
        assert sent_per_second > 500
        assert messages_per_second > 500
