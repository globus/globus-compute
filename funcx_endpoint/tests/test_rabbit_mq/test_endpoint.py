import json
import logging
import multiprocessing
import random
import uuid

import pika

from funcx_endpoint.endpoint.rabbit_mq import (
    ResultQueuePublisher,
    ResultQueueSubscriber,
    TaskQueuePublisher,
    TaskQueueSubscriber,
)

ENDPOINT_ID = "231fab4e-630a-4d76-bbbb-cf0b4aedbdf9"


def start_task_q_subscriber(
    endpoint_id: str,
    out_queue: multiprocessing.Queue,
    disconnect_event: multiprocessing.Event,
    conn_params: pika.connection.Parameters,
):

    task_q = TaskQueueSubscriber(
        conn_params,
        external_queue=out_queue,
        kill_event=disconnect_event,
        endpoint_uuid=endpoint_id,
    )
    task_q.start()
    return task_q


def start_result_q_publisher(
    endpoint_id, conn_params: pika.connection.Parameters
) -> ResultQueuePublisher:
    result_pub = ResultQueuePublisher(
        endpoint_id=endpoint_id, pika_conn_params=conn_params
    )
    result_pub.connect()
    return result_pub


def start_task_q_publisher(
    endpoint_id: str, conn_params: pika.connection.Parameters
) -> TaskQueuePublisher:
    task_q_pub = TaskQueuePublisher(
        endpoint_uuid=endpoint_id, pika_conn_params=conn_params
    )
    task_q_pub.connect()
    return task_q_pub


def start_result_q_subscriber(
    queue: multiprocessing.Queue, conn_params: pika.connection.Parameters
) -> ResultQueueSubscriber:
    result_q = ResultQueueSubscriber(pika_conn_params=conn_params, external_queue=queue)
    result_q.start()
    return result_q


def run_async_service():
    """Run a task_q_publisher and result_q_subscriber mocking a simple service"""
    task_q_pub = start_task_q_publisher(endpoint_id=ENDPOINT_ID)
    task_q_pub.queue_purge()
    result_q = multiprocessing.Queue()
    result_q_proc = multiprocessing.Process(
        target=start_result_q_subscriber, args=(result_q,)
    )
    result_q_proc.start()
    logging.warning("SERVICE: Here")
    for _i in range(10):
        try:
            message = {
                "task_id": str(uuid.uuid4()),
                "task_buf": "TASKBUF TASKBUF",
            }
            b_message = json.dumps(message).encode()
            logging.warning("SERVICE: Trying to publish message")
            task_q_pub.publish(b_message)
            logging.warning(f"SERVICE: Published message: {message}")
            from_ep, reply_message = result_q.get(timeout=5)
            logging.warning(f"SERVICE: Received result message: {reply_message}")

            assert from_ep == ENDPOINT_ID
            assert reply_message == b_message
        except Exception:
            logging.exception("Caught error")

    result_q_proc.terminate()
    task_q_pub.close()


def test_simple_roundtrip(conn_params: pika.connection.Parameters):

    task_pub = start_task_q_publisher(endpoint_id=ENDPOINT_ID, conn_params=conn_params)
    task_pub.queue_purge()
    result_pub = start_result_q_publisher(
        endpoint_id=ENDPOINT_ID, conn_params=conn_params
    )
    result_pub._channel.queue_purge("results")
    task_q, result_q = multiprocessing.Queue(), multiprocessing.Queue()
    task_fail_event = multiprocessing.Event()

    task_q_proc = start_task_q_subscriber(
        ENDPOINT_ID,
        out_queue=task_q,
        disconnect_event=task_fail_event,
        conn_params=conn_params,
    )

    result_q_proc = start_result_q_subscriber(result_q, conn_params=conn_params)

    message = f"Hello {random.randint(0,2**10)}".encode()
    logging.warning(f"Sending message: {message}")
    task_pub.publish(message)
    task_message = task_q.get(timeout=2)
    assert message == task_message

    result_pub.publish(task_message)
    result_message = result_q.get(timeout=2)

    assert result_message == (ENDPOINT_ID, message)

    task_pub.close()
    result_pub.close()
    task_q_proc.terminate()
    result_q_proc.terminate()
