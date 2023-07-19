import concurrent.futures
import logging
import random
import time
import uuid
from queue import Queue

import pytest
from globus_compute_endpoint.engines import GlobusComputeEngine
from globus_compute_endpoint.strategies import SimpleStrategy
from parsl.providers import LocalProvider
from tests.utils import double

logger = logging.getLogger(__name__)


@pytest.fixture
def gc_engine_scaling(tmp_path):
    ep_id = uuid.uuid4()
    engine = GlobusComputeEngine(
        address="127.0.0.1",
        heartbeat_period_s=1,
        heartbeat_threshold=1,
        provider=LocalProvider(
            init_blocks=0,
            min_blocks=0,
            max_blocks=1,
        ),
        strategy=SimpleStrategy(interval=0.1, max_idletime=0),
    )
    queue = Queue()
    engine.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=queue)

    yield engine
    engine.shutdown()


@pytest.fixture
def gc_engine_non_scaling(tmp_path):
    ep_id = uuid.uuid4()
    engine = GlobusComputeEngine(
        address="127.0.0.1",
        heartbeat_period_s=1,
        heartbeat_threshold=1,
        provider=LocalProvider(
            init_blocks=1,
            min_blocks=1,
            max_blocks=1,
        ),
        strategy=None,
    )
    queue = Queue()
    engine.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=queue)

    yield engine
    engine.shutdown()


def test_engine_submit_init_0(gc_engine_scaling):
    """Test engine scaling from 0 blocks with GCE"""
    engine = gc_engine_scaling
    max_idletime = engine.strategy.max_idletime

    # At the start there should be 0 managers
    outstanding = engine.get_outstanding_breakdown()
    assert len(outstanding) == 1, "Expected only interchange"

    # Run a function to trigger scale_out and confirm via breakdown
    param = random.randint(1, 100)
    future = engine._submit(double, param)
    assert isinstance(future, concurrent.futures.Future)
    assert future.result() == param * 2

    outstanding = engine.get_outstanding_breakdown()
    assert len(outstanding) == 2, "Expected 1 manager + interchange"

    # With 0 tasks and excess workers we should expect scale_down
    # While scale_down might be triggered it appears to take 1s
    # lowest heartbeat period to detect a manager going down
    while True:
        managers = engine.executor.connected_managers()
        if len(managers) == 0:
            break
        idle_time = managers[0]["idle_duration"]
        assert idle_time <= max_idletime + 1, "Manager exceeded idletime"
        time.sleep(0.1)


def test_engine_no_scaling(gc_engine_non_scaling):
    """Confirm that Engine works with fixes # of blocks"""

    engine = gc_engine_non_scaling
    assert engine.strategy is None

    # At the start there should be 0 managers
    outstanding = engine.get_outstanding_breakdown()
    assert len(outstanding) == 1, "Expected only interchange"

    # Run a function to trigger scale_out and confirm via breakdown
    param = random.randint(1, 100)
    future = engine._submit(double, param)
    assert isinstance(future, concurrent.futures.Future)
    assert future.result() == param * 2

    # Confirm that there's 1 manager
    outstanding = engine.get_outstanding_breakdown()
    assert len(outstanding) == 2, "Expecting 1 manager+interchange"
