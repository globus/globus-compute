import json
import multiprocessing
import threading
import time
import uuid

import pytest
from globus_compute_endpoint.endpoint.rabbit_mq import TaskQueueSubscriber

_MOCK_BASE = "globus_compute_endpoint.endpoint.rabbit_mq.task_queue_subscriber."


def test_synch(start_task_q_publisher, start_task_q_subscriber, count=10):
    """Open publisher, and publish to task_q, then open subscriber a fetch"""
    task_q_pub = start_task_q_publisher()

    messages = []
    for i in range(count):
        message = {"task_id": str(uuid.uuid4()), "result": f"foo-{i}"}
        b_message = json.dumps(message).encode()
        messages.append(b_message)
        task_q_pub.publish(b_message)

    tasks_out = multiprocessing.Queue()
    start_task_q_subscriber(queue=tasks_out)
    for i in range(count):
        _, message = tasks_out.get()
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
    quiesce_event = multiprocessing.Event()
    proc = start_task_q_subscriber(queue=tasks_out, quiesce_event=quiesce_event)
    for i in range(10):
        _, message = tasks_out.get()
        assert messages[i] == message

    # Terminate the connection
    proc.stop()

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
    quiesce_event.clear()
    start_task_q_subscriber(queue=tasks_out, quiesce_event=quiesce_event)

    for i in range(10):
        _, message = tasks_out.get()
        assert messages[i] == message


def test_exclusive_subscriber(mocker, start_task_q_publisher, start_task_q_subscriber):
    """2 subscribers connect, only first one should get any messages"""
    task_q_pub = start_task_q_publisher()

    # Start two subscribers on the same rabbit queue
    tasks_out_1, tasks_out_2 = multiprocessing.Queue(), multiprocessing.Queue()
    start_task_q_subscriber(queue=tasks_out_1)
    time.sleep(0.1)

    mocker.patch(f"{_MOCK_BASE}logger")
    start_task_q_subscriber(queue=tasks_out_2)

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

    # Confirm that the first subscriber received all the messages
    for i in range(10):
        _, message = tasks_out_1.get(timeout=1)
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
        _, x = tasks_out.get()
        delta = time.time() - start_t
        latency.append(delta)
        assert b_message == x

    avg_latency = sum(latency) / len(latency)
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

        # each size should record at least 500 messages per second even in an
        # untuned context with other processes running
        # slower than that indicates that a serious performance regression has
        # been introduced
        assert sent_per_second > 500
        assert messages_per_second > 500


@pytest.mark.parametrize("pathway", ["connection", "channel"])
def test_graceful_shutdown_if_closed_unexpectedly(
    mocker, task_queue_info, ensure_task_queue, pathway: str
):
    def _run_it():
        def _stop_connection_now(tqs: TaskQueueSubscriber):
            _now = time.monotonic()
            while not tqs._channel and time.monotonic() - _now < 3:
                time.sleep(0.05)
            tqs.status = -123  # Just something that's not the sentinel
            attr = f"_{pathway}"
            getattr(tqs, attr).close()

        ensure_task_queue(queue_opts={"queue": task_queue_info["queue"]})
        tqs = TaskQueueSubscriber(
            endpoint_id="abc",
            queue_info=task_queue_info,
            external_queue=mocker.Mock(),
            quiesce_event=threading.Event(),
        )
        threading.Thread(target=_stop_connection_now, args=(tqs,), daemon=True).start()
        tqs.run()
        exit(0 if tqs._cleanup_complete.is_set() is True else 1)

    p = multiprocessing.Process(target=_run_it)
    p.start()
    p.join(timeout=4)
    assert p.exitcode == 0


def test_terminate(start_task_q_subscriber):
    task_q = start_task_q_subscriber()
    time.sleep(0.1)
    task_q.stop()
    with pytest.raises(ValueError):
        # Expected to raise ValueError since the process should
        # be terminated at this point from the close() call
        task_q.terminate()
