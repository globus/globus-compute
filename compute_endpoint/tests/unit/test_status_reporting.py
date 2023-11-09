import logging

import parsl
import pytest
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import EPStatusReport
from globus_compute_endpoint import engines
from pytest_mock import MockFixture

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "engine_type",
    (engines.ProcessPoolEngine, engines.ThreadPoolEngine, engines.GlobusComputeEngine),
)
def test_status_reporting(engine_type, engine_runner):
    engine = engine_runner(engine_type)

    report = engine.get_status_report()
    assert isinstance(report, EPStatusReport)

    results_q = engine.results_passthrough

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


def test_gcengine_status_report(mocker: MockFixture, engine_runner: callable):
    managers = [
        {
            "manager": "f6826ed3c7cd",
            "block_id": "0",
            "worker_count": 12,
            "tasks": 5,
            "idle_duration": 0.0,
            "active": True,
        },
        {
            "manager": "e6428de3c7ce",
            "block_id": "1",
            "worker_count": 12,
            "tasks": 15,
            "idle_duration": 0.0,
            "active": False,
        },
        {
            "manager": "d63f7ee5c78d",
            "block_id": "2",
            "worker_count": 12,
            "tasks": 0,
            "idle_duration": 3.8,
            "active": True,
        },
    ]
    mocker.patch.object(
        parsl.HighThroughputExecutor, "connected_managers", lambda x: managers
    )
    mocker.patch.object(parsl.HighThroughputExecutor, "outstanding", 25)

    engine: engines.GlobusComputeEngine = engine_runner(engines.GlobusComputeEngine)
    status_report = engine.get_status_report()
    info = status_report.global_state["info"]

    assert info["managers"] == 3
    assert info["active_managers"] == 2
    assert info["total_workers"] == 36
    assert info["idle_workers"] == 19
    assert info["pending_tasks"] == 5
    assert info["outstanding_tasks"] == 25
    assert info["scaling_enabled"] == engine.executor.provider.max_blocks > 0
    assert info["mem_per_worker"] == engine.executor.mem_per_worker
    assert info["cores_per_worker"] == engine.executor.cores_per_worker
    assert info["prefetch_capacity"] == engine.executor.prefetch_capacity
    assert info["max_blocks"] == engine.executor.provider.max_blocks
    assert info["min_blocks"] == engine.executor.provider.min_blocks
    assert info["max_workers_per_node"] == engine.executor.max_workers
    assert info["nodes_per_block"] == engine.executor.provider.nodes_per_block
    assert info["heartbeat_period"] == engine.executor.heartbeat_period
