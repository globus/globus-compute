import logging

import pytest
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import EPStatusReport
from globus_compute_endpoint import engines

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("engine_type", ["proc_pool", "thread_pool", "gc"])
def test_status_reporting(
    proc_pool_engine: engines.ProcessPoolEngine,
    thread_pool_engine: engines.ThreadPoolEngine,
    gc_engine: engines.GlobusComputeEngine,
    engine_heartbeat: float,
    engine_type: str,
):
    if engine_type == "proc_pool":
        engine = proc_pool_engine
    elif engine_type == "thread_pool":
        engine = thread_pool_engine
    else:
        engine = gc_engine

    report = engine.get_status_report()
    assert isinstance(report, EPStatusReport)

    results_q = engine.results_passthrough

    assert engine._status_report_thread.reporting_period == engine_heartbeat

    # Flush queue to start
    while not results_q.empty():
        results_q.get()

    # Confirm heartbeats in regular intervals
    for _i in range(3):
        message = results_q.get(timeout=0.2)
        assert isinstance(message, bytes)

        report = messagepack.unpack(message)
        assert isinstance(report, EPStatusReport)
