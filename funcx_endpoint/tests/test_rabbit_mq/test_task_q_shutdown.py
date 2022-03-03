import logging
import multiprocessing
import time

import pika
import pytest

from funcx_endpoint.endpoint.rabbit_mq import TaskQueueSubscriber

LOG_FORMAT = "%(levelname) -10s %(asctime)s %(name) -20s %(lineno) -5d: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

CONN_PARAMS = pika.URLParameters("amqp://guest:guest@localhost:5672/")
ENDPOINT_ID = "95abffc0-11cc-43cf-8b9b-c1acadf6f877"


def test_terminate():

    out_queue = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()

    task_q = TaskQueueSubscriber(
        CONN_PARAMS,
        external_queue=out_queue,
        kill_event=disconnect_event,
        endpoint_uuid=ENDPOINT_ID,
    )
    logger.warning("Start")
    task_q.start()
    time.sleep(3)
    logger.warning("Calling terminate")
    task_q.close()
    with pytest.raises(ValueError):
        # Expected to raise ValueError since the process should
        # be terminated at this point from the close() call
        task_q.terminate()

    return task_q
