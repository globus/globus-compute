import logging

import pytest
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import EPStatusReport
from globus_compute_endpoint import engines

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "engine_type",
    (engines.ProcessPoolEngine, engines.ThreadPoolEngine, engines.GlobusComputeEngine),
)
def test_status_reporting(engine_type, engine_runner, engine_heartbeat: int):
    engine = engine_runner(engine_type)

    report = engine.get_status_report()
    assert isinstance(report, EPStatusReport)

    results_q = engine.results_passthrough

    assert engine._status_report_thread.reporting_period == engine_heartbeat

    # Flush queue to start
    while not results_q.empty():
        results_q.get()

    # Confirm heartbeats in regular intervals
    for _i in range(3):
        q_msg = results_q.get(timeout=2)
        assert isinstance(q_msg, dict)

        message = q_msg["message"]
        report = messagepack.unpack(message)
        assert isinstance(report, EPStatusReport)
