import uuid
from queue import Queue

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GlobusComputeEngine
from globus_compute_endpoint.strategies import SimpleStrategy
from globus_compute_sdk.serialize import ComputeSerializer
from parsl.executors.high_throughput.interchange import ManagerLost
from parsl.providers import LocalProvider
from tests.utils import ez_pack_function, kill_manager, succeed_after_n_runs


@pytest.fixture
def gc_engine_with_retries(tmp_path):
    ep_id = uuid.uuid4()
    engine = GlobusComputeEngine(
        address="127.0.0.1",
        max_workers=1,
        heartbeat_period=1,
        heartbeat_threshold=1,
        max_retries_on_system_failure=0,
        provider=LocalProvider(
            init_blocks=0,
            min_blocks=0,
            max_blocks=1,
        ),
        strategy=SimpleStrategy(interval=0.1, max_idletime=0),
    )
    engine._status_report_thread.reporting_period = 1
    queue = Queue()
    engine.start(endpoint_id=ep_id, run_dir=tmp_path, results_passthrough=queue)
    yield engine
    engine.shutdown()


def test_gce_kill_manager(gc_engine_with_retries):
    engine = gc_engine_with_retries
    engine.max_retries_on_system_failure = 0
    queue = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()

    # Confirm error message for ManagerLost
    task_body = ez_pack_function(serializer, kill_manager, (), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(task_id=task_id, task_buffer=task_body)
    )

    future = engine.submit(task_id, task_message)

    with pytest.raises(ManagerLost):
        future.result()

    flag = False
    for _i in range(4):
        q_msg = queue.get(timeout=2)
        assert isinstance(q_msg, dict)

        packed_result_q = q_msg["message"]
        result = messagepack.unpack(packed_result_q)
        if isinstance(result, messagepack.message_types.Result):
            assert result.task_id == task_id
            if result.error_details and "ManagerLost" in result.data:
                flag = True
                break

    assert flag, "Result message missing"


def test_success_after_1_fail(gc_engine_with_retries, tmp_path):
    engine = gc_engine_with_retries
    engine.max_retries_on_system_failure = 2
    fail_count = 1
    queue = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(
        serializer, succeed_after_n_runs, (tmp_path,), {"fail_count": fail_count}
    )
    task_message = messagepack.pack(
        messagepack.message_types.Task(task_id=task_id, task_buffer=task_body)
    )
    engine.submit(task_id, task_message)

    flag = False
    for _i in range(10):
        q_msg = queue.get(timeout=5)
        assert isinstance(q_msg, dict)

        packed_result_q = q_msg["message"]
        result = messagepack.unpack(packed_result_q)
        if isinstance(result, messagepack.message_types.Result):
            assert result.task_id == task_id
            assert result.error_details is None
            flag = True
            break

    assert flag, "Expected result packet, but none received"


def test_repeated_fail(gc_engine_with_retries, tmp_path):
    engine = gc_engine_with_retries
    engine.max_retries_on_system_failure = 2
    fail_count = 3
    queue = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(
        serializer, succeed_after_n_runs, (tmp_path,), {"fail_count": fail_count}
    )
    task_message = messagepack.pack(
        messagepack.message_types.Task(task_id=task_id, task_buffer=task_body)
    )
    engine.submit(task_id, task_message)

    flag = False
    for _i in range(10):
        q_msg = queue.get(timeout=5)
        assert isinstance(q_msg, dict)

        packed_result_q = q_msg["message"]
        result = messagepack.unpack(packed_result_q)
        if isinstance(result, messagepack.message_types.Result):
            assert result.task_id == task_id
            assert result.error_details
            assert "ManagerLost" in result.data
            count = result.data.count("Traceback from attempt")
            assert count == fail_count, "Got incorrect # of failure reports"
            assert "final attempt" in result.data
            flag = True
            break

    assert flag, "Expected ManagerLost in failed result.data, but none received"


def test_default_retries_is_0():
    engine = GlobusComputeEngine(address="127.0.0.1")
    assert engine.max_retries_on_system_failure == 0, "Users must knowingly opt-in"
