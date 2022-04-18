import logging
import multiprocessing
import random


def test_simple_roundtrip(
    start_task_q_publisher,
    start_task_q_subscriber,
    start_result_q_subscriber,
    start_result_q_publisher,
    default_endpoint_id,
):
    task_pub = start_task_q_publisher()
    result_pub = start_result_q_publisher()
    task_q, result_q = multiprocessing.Queue(), multiprocessing.Queue()

    start_task_q_subscriber(queue=task_q)
    start_result_q_subscriber(queue=result_q)

    message = f"Hello {random.randint(0,2**10)}".encode()
    logging.warning(f"Sending message: {message}")
    task_pub.publish(message)
    task_message = task_q.get(timeout=2)
    assert message == task_message

    result_pub.publish(task_message)
    result_message = result_q.get(timeout=2)

    assert result_message == (default_endpoint_id, message)
