import queue


def test_simple_roundtrip(
    flush_results,
    start_task_q_publisher,
    start_result_q_publisher,
    start_task_q_subscriber,
    start_result_q_subscriber,
    randomstring,
):
    task_q, result_q = queue.SimpleQueue(), queue.SimpleQueue()

    # Start the publishers *first* as that route creates the queues
    task_pub = start_task_q_publisher()
    result_pub = start_result_q_publisher()

    start_task_q_subscriber(task_queue=task_q)
    start_result_q_subscriber(result_q=result_q)

    message = f"Hello test_simple_roundtrip: {randomstring()}".encode()
    task_pub.publish(message)
    _, _, task_message = task_q.get(timeout=2)
    assert message == task_message

    result_pub.publish(task_message)
    _, result_message = result_q.get(timeout=2)

    _, expected = (result_pub.queue_info["test_routing_key"], message)
    assert result_message == expected
