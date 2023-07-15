import logging
import multiprocessing
import uuid
from queue import Queue

import pytest
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import EPStatusReport
from globus_compute_endpoint import engines

logger = logging.getLogger(__name__)

HEARTBEAT_PERIOD = 0.1


@pytest.fixture
def proc_pool_engine(tmp_path):
    ep_id = uuid.uuid4()
    engine = engines.ProcessPoolEngine(
        max_workers=2, heartbeat_period_s=HEARTBEAT_PERIOD
    )
    queue = multiprocessing.Queue()
    engine.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=queue)

    yield engine
    engine.shutdown()


@pytest.fixture
def thread_pool_engine(tmp_path):
    ep_id = uuid.uuid4()
    engine = engines.ThreadPoolEngine(
        max_workers=2, heartbeat_period_s=HEARTBEAT_PERIOD
    )

    queue = Queue()
    engine.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=queue)

    yield engine
    engine.shutdown()


@pytest.fixture
def gc_engine(tmp_path):
    ep_id = uuid.uuid4()
    engine = engines.GlobusComputeEngine(
        address="127.0.0.1",
        heartbeat_period_s=HEARTBEAT_PERIOD,
        heartbeat_threshold=1,
    )
    queue = multiprocessing.Queue()
    engine.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=queue)

    yield engine
    engine.shutdown()


@pytest.mark.parametrize("engine_type", ["proc_pool", "thread_pool", "gc"])
def test_status_reporting(
    proc_pool_engine: engines.ProcessPoolEngine,
    thread_pool_engine: engines.ThreadPoolEngine,
    gc_engine: engines.GlobusComputeEngine,
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

    assert engine._status_report_thread.reporting_period == HEARTBEAT_PERIOD

    # Flush queue to start
    while not results_q.empty():
        results_q.get()

    # Confirm heartbeats in regular intervals
    for _i in range(3):
        message = results_q.get(timeout=0.2)
        assert isinstance(message, bytes)

        report = messagepack.unpack(message)
        assert isinstance(report, EPStatusReport)
