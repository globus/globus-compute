import logging
import multiprocessing
import os
import shutil
import uuid

import pytest
from funcx_common import messagepack
from funcx_common.messagepack.message_types import EPStatusReport
from pytest import fixture

from funcx_endpoint.engines import GlobusComputeEngine, ProcessPoolEngine

logger = logging.getLogger(__name__)

HEARTBEAT_PERIOD = 0.1


@fixture
def proc_pool_engine():
    ep_id = uuid.uuid4()
    executor = ProcessPoolEngine(max_workers=2, heartbeat_period=HEARTBEAT_PERIOD)
    queue = multiprocessing.Queue()
    executor.start(endpoint_id=ep_id, run_dir="/tmp", results_passthrough=queue)

    yield executor
    executor.shutdown()


@fixture
def gc_engine():
    ep_id = uuid.uuid4()
    executor = GlobusComputeEngine(
        address="127.0.0.1",
        heartbeat_period=HEARTBEAT_PERIOD,
        heartbeat_threshold=1,
    )
    queue = multiprocessing.Queue()
    tempdir = "/tmp/HTEX_logs"
    os.makedirs(tempdir, exist_ok=True)
    executor.start(endpoint_id=ep_id, run_dir=tempdir, results_passthrough=queue)

    yield executor
    executor.shutdown()
    shutil.rmtree(tempdir, ignore_errors=True)


@pytest.mark.parametrize("x", ["gc_engine", "proc_pool_engine"])
def test_status_reporting(x, gc_engine, proc_pool_engine):
    if x == "gc_engine":
        executor = gc_engine
    elif x == "proc_pool_engine":
        executor = proc_pool_engine

    report = executor.get_status_report()
    assert isinstance(report, EPStatusReport)

    results_q = executor.results_passthrough

    assert executor._status_report_thread.reporting_period == HEARTBEAT_PERIOD

    # Flush queue to start
    while not results_q.empty():
        results_q.get()

    # Confirm heartbeats in regular intervals
    for _i in range(3):
        message = results_q.get(timeout=0.2)
        assert isinstance(message, bytes)

        report = messagepack.unpack(message)
        assert isinstance(report, EPStatusReport)
