import logging
import multiprocessing
import time

import pytest

from funcx_endpoint.endpoint.rabbit_mq import TaskQueueSubscriber

ENDPOINT_ID = "95abffc0-11cc-43cf-8b9b-c1acadf6f877"


def test_terminate(conn_params):

    out_queue = multiprocessing.Queue()
    disconnect_event = multiprocessing.Event()

    task_q = TaskQueueSubscriber(
        conn_params,
        external_queue=out_queue,
        kill_event=disconnect_event,
        endpoint_uuid=ENDPOINT_ID,
    )
    logging.warning("Start")
    task_q.start()
    time.sleep(3)
    logging.warning("Calling terminate")
    task_q.stop()
    with pytest.raises(ValueError):
        # Expected to raise ValueError since the process should
        # be terminated at this point from the close() call
        task_q.terminate()

    return task_q
