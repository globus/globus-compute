import logging
import queue
import time

import pytest
from globus_compute_endpoint.engines.base import ReportingThread

logger = logging.getLogger(__name__)


def test_reporting_thread():
    def callback(result_queue: queue.Queue):
        # Pop item into queue until queue is full
        # after which it raises queue.Full
        result_queue.put("42", block=False)

    # We expect 5 items to be popped into the queue
    # before it throws an exception
    result_q = queue.Queue(maxsize=5)
    rt = ReportingThread(target=callback, args=[result_q], reporting_period=0.01)
    rt.start()
    # Give enough time for the callbacks to be executed N times
    time.sleep(0.2)
    rt.stop()

    assert result_q.qsize() == 5
    assert rt.status.exception()
    with pytest.raises(queue.Full):
        rt.status.result()
