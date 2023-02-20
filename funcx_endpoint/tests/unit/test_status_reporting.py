import logging
import multiprocessing
import os
import shutil
import uuid

import pytest
from funcx_common import messagepack
from funcx_common.messagepack.message_types import EPStatusReport
from pytest import fixture

from funcx_endpoint.executors import GCExecutor, ProcessPoolExecutor

logger = logging.getLogger(__name__)

HEARTBEAT_PERIOD = 0.1


@fixture
def proc_pool_executor():
    ep_id = uuid.uuid4()
    executor = ProcessPoolExecutor(max_workers=2, heartbeat_period=HEARTBEAT_PERIOD)
    queue = multiprocessing.Queue()
    executor.start(ep_id, run_dir="/tmp", results_passthrough=queue)

    yield executor
    executor.shutdown()


@fixture
def gc_executor():
    ep_id = uuid.uuid4()
    executor = GCExecutor(
        address="127.0.0.1",
        heartbeat_period=HEARTBEAT_PERIOD,
        heartbeat_threshold=1,
    )
    queue = multiprocessing.Queue()
    tempdir = "/tmp/HTEX_logs"
    os.makedirs(tempdir, exist_ok=True)
    executor.start(ep_id, run_dir=tempdir, results_passthrough=queue)

    yield executor
    executor.shutdown()
    shutil.rmtree(tempdir, ignore_errors=True)


@pytest.mark.parametrize("x", ["gc_executor", "proc_pool_executor"])
def test_status_reporting(x, gc_executor, proc_pool_executor):
    if x == "gc_executor":
        executor = gc_executor
    elif x == "proc_pool_executor":
        executor = proc_pool_executor

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
