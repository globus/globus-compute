import time
from unittest.mock import patch

from globus_compute_endpoint.engines.base import ReportingThread
from globus_compute_endpoint.engines.globus_compute import GlobusComputeEngine
from globus_compute_endpoint.engines.thread_pool import ThreadPoolEngine

G_COUNTER = 0
REPORTING_PERIOD = 0.1


def counter():
    global G_COUNTER
    G_COUNTER += 1


def test_default_period():
    """Confirm that the default reporting period is maintained"""

    thread = ReportingThread(target=counter, args=[])
    assert thread.reporting_period == 30.0

    thread_pool = ThreadPoolEngine()
    thread = thread_pool._status_report_thread
    assert thread.reporting_period == 30.0

    gce = GlobusComputeEngine(address="127.0.0.1", heartbeat_period=1)
    thread = gce._status_report_thread
    assert thread.reporting_period == 30.0
    assert gce.executor.heartbeat_period == 1


def test_reporting_period():
    thread = ReportingThread(target=counter, args=[])

    with patch.object(thread, "reporting_period", REPORTING_PERIOD):
        assert thread.reporting_period == REPORTING_PERIOD
        thread.start()
        time.sleep(0.25)
        thread.stop()

    assert G_COUNTER == 3
