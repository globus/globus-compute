import os
import queue
import uuid

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.executors import HighThroughputExecutor
from globus_compute_sdk.serialize import ComputeSerializer
from tests.utils import div_zero, double, ez_pack_function, kill_manager


@pytest.fixture
def htex():
    ep_id = uuid.uuid4()
    executor = HighThroughputExecutor(
        address="127.0.0.1",
        heartbeat_period=1,
        heartbeat_threshold=1,
        worker_debug=True,
    )
    q = queue.Queue()
    tempdir = "/tmp/HTEX_logs"

    os.makedirs(tempdir, exist_ok=True)
    executor.start(endpoint_id=ep_id, run_dir=tempdir, results_passthrough=q)

    yield executor
    executor.shutdown()
    # shutil.rmtree(tempdir, ignore_errors=True)


@pytest.mark.skip("Skip until HTEX has been fixed up")
def test_htex_submit_raw(htex):
    """Testing the HighThroughputExecutor/Engine"""
    engine = htex

    q = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, double, (3,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    # HTEX doesn't give you a future back
    engine.submit_raw(task_message)

    # Confirm that the same result got back though the queue
    for _i in range(3):
        packed_result_q = q.get(timeout=5)
        assert isinstance(
            packed_result_q, bytes
        ), "Expected bytes from the passthrough_q"

        result = messagepack.unpack(packed_result_q)
        # Handle a sneaky EPStatusReport that popped in ahead of the result
        if isinstance(result, messagepack.message_types.EPStatusReport):
            continue

        # At this point the message should be the result
        assert result.task_id == task_id

        final_result = serializer.deserialize(result.data)
        assert final_result == 6, f"Expected 6, but got: {final_result}"
        break


@pytest.mark.skip("Skip until HTEX has been fixed up")
def test_htex_submit_raw_exception(htex):
    """Testing the HighThroughputExecutor/Engine with a remote side exception"""
    engine = htex

    q = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, div_zero, (3,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    # HTEX doesn't give you a future back
    engine.submit_raw(task_message)

    # Confirm that the same result got back though the queue
    for _i in range(3):
        packed_result_q = q.get(timeout=5)
        assert isinstance(
            packed_result_q, bytes
        ), "Expected bytes from the passthrough_q"

        result = messagepack.unpack(packed_result_q)
        # Handle a sneaky EPStatusReport that popped in ahead of the result
        if isinstance(result, messagepack.message_types.EPStatusReport):
            continue

        # At this point the message should be the result
        assert result.task_id == task_id
        assert result.error_details
        break


@pytest.mark.skip("Skip until HTEX has been fixed up")
def test_htex_manager_lost(htex):
    """Testing the HighThroughputExecutor/Engine"""
    engine = htex

    q = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, kill_manager, (), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    # HTEX doesn't give you a future back
    engine.submit_raw(task_message)

    # Confirm that the same result got back though the queue
    for _i in range(10):
        # We need a longer timeout to detect manager fail
        packed_result_q = q.get(timeout=5)
        assert isinstance(
            packed_result_q, bytes
        ), "Expected bytes from the passthrough_q"

        result = messagepack.unpack(packed_result_q)
        # Handle a sneaky EPStatusReport that popped in ahead of the result
        if isinstance(result, messagepack.message_types.EPStatusReport):
            continue

        # At this point the message should be the result
        assert result.task_id == task_id

        assert result.error_details.code == "RemoteExecutionError"
        assert "ManagerLost" in result.data
        break
