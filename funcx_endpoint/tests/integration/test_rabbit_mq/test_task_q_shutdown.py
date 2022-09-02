import logging
import time

import pytest


def test_terminate(start_task_q_subscriber):
    task_q = start_task_q_subscriber()
    time.sleep(1)
    task_q.stop()
    logging.warning("Calling terminate")
    with pytest.raises(ValueError):
        # Expected to raise ValueError since the process should
        # be terminated at this point from the close() call
        task_q.terminate()
