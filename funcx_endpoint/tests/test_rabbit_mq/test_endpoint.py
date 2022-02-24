import json
import logging
import multiprocessing
import random
import uuid

import pika
import pytest

from funcx_endpoint.endpoint.rabbit_mq import (
    ResultQueuePublisher,
    ResultQueueSubscriber,
    TaskQueuePublisher,
    TaskQueueSubscriber,
)

LOG_FORMAT = "%(levelname) -10s %(asctime)s %(name) -20s %(lineno) -5d: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

ENDPOINT_ID = "231fab4e-630a-4d76-bbbb-cf0b4aedbdf9"
CONN_PARAMS = pika.URLParameters("amqp://guest:guest@localhost:5672/%2F")


def start_task_q_subscriber(
    out_queue: multiprocessing.Queue, disconnect_event: multiprocessing.Event
):
    task_q = TaskQueueSubscriber(
        CONN_PARAMS,
        external_queue=out_queue,
        kill_event=disconnect_event,
        endpoint_uuid=ENDPOINT_ID,
    )
    task_q.start()
    return task_q


def start_result_q_publisher(endpoint_id) -> ResultQueuePublisher:
    result_pub = ResultQueuePublisher(
        endpoint_id=endpoint_id, pika_conn_params=CONN_PARAMS
    )
    result_pub.connect()
    return result_pub


def start_task_q_publisher(endpoint_id: str) -> TaskQueuePublisher:
    task_q_pub = TaskQueuePublisher(
        endpoint_uuid=endpoint_id, pika_conn_params=CONN_PARAMS
    )
    task_q_pub.connect()
    return task_q_pub


def start_result_q_subscriber(queue: multiprocessing.Queue) -> ResultQueueSubscriber:
    kill_event = multiprocessing.Event()
    result_q = ResultQueueSubscriber(
        pika_conn_params=CONN_PARAMS, external_queue=queue, kill_event=kill_event
    )
    result_q.start()
    return result_q


def run_async_service():
    """Run a task_q_publisher and result_q_subscriber mocking a simple service"""
    task_q_pub = start_task_q_publisher(endpoint_id=ENDPOINT_ID)
    task_q_pub.queue_purge()
    result_q = multiprocessing.Queue()
    result_q_proc = start_result_q_subscriber(result_q)

    logger.warning("SERVICE: Here")
    for _i in range(10):
        try:
            message = {
                "task_id": str(uuid.uuid4()),
                "task_buf": "TASKBUF TASKBUF",
            }
            b_message = json.dumps(message).encode()
            logger.warning("SERVICE: Trying to publish message")
            task_q_pub.publish(b_message)
            logger.warning(f"SERVICE: Published message: {message}")
            from_ep, reply_message = result_q.get(timeout=2)
            logger.warning(f"SERVICE: Received result message: {reply_message}")

            assert from_ep == ENDPOINT_ID
            assert reply_message == b_message
        except (AssertionError, TimeoutError):
            raise
        except Exception:
            # Catching and logging so that errors from this process are logged
            # rather than silently ignored upon process fail/exit
            logger.exception("Caught error")
        finally:
            result_q_proc.terminate()
            task_q_pub.close()


@pytest.mark.skip
def test_mock_endpoint():

    service = multiprocessing.Process(target=run_async_service, args=())
    service.start()

    tasks_out = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()

    # Listen for 10 messages
    proc = start_task_q_subscriber(tasks_out, disconnect_event)
    result_q_pub = start_result_q_publisher(ENDPOINT_ID)
    result_q_pub._queue_purge()

    logger.warning("ENDPOINT: Here")

    for _i in range(10):
        logger.warning("ENDPOINT: Waiting for task: ")
        b_message = tasks_out.get(timeout=2)
        logger.warning(f"ENDPOINT: Received message: {b_message}")
        logger.warning("ENDPOINT: Publishing message to results_q")
        result_q_pub.publish(b_message)

    service.terminate()
    proc.terminate()


def test_simple_roundtrip():

    task_pub = start_task_q_publisher(endpoint_id=ENDPOINT_ID)
    task_pub.queue_purge()
    result_pub = start_result_q_publisher(endpoint_id=ENDPOINT_ID)
    result_pub._queue_purge()  # Only tests should be able to do this
    task_q, result_q = multiprocessing.Queue(), multiprocessing.Queue()
    task_fail_event = multiprocessing.Event()

    task_q_proc = start_task_q_subscriber(
        out_queue=task_q, disconnect_event=task_fail_event
    )
    result_q_proc = start_result_q_subscriber(result_q)

    message = f"Hello {random.randint(0,2**10)}".encode()
    logger.warning(f"Sending message: {message}")
    task_pub.publish(message)
    task_message = task_q.get(timeout=2)
    assert message == task_message

    result_pub.publish(task_message)
    result_message = result_q.get(timeout=2)

    assert result_message == (ENDPOINT_ID, message)
    logger.warning("Roundtrip complete")
    task_pub.close()
    result_pub.close()
    task_q_proc.terminate()
    result_q_proc.terminate()
