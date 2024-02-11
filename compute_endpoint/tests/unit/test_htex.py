import os
import queue
import threading
import typing as t
import uuid
from unittest import mock

import dill
import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import HighThroughputEngine
from globus_compute_endpoint.engines.high_throughput.messages import (
    Task as InternalTask,
)
from globus_compute_sdk.serialize import ComputeSerializer
from pytest_mock import MockFixture
from tests.utils import div_zero, double, ez_pack_function, kill_manager, try_assert


@pytest.fixture
def htex():
    ep_id = uuid.uuid4()
    executor = HighThroughputEngine(
        address="127.0.0.1",
        heartbeat_period=1,
        heartbeat_threshold=2,
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


def test_engine_submit_container_location(
    mocker: MockFixture, htex: HighThroughputEngine
):
    engine = htex
    task_id = uuid.uuid4()
    container_id = uuid.uuid4()
    container_type = "singularity"
    container_loc = "/path/to/container"
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, double, (10,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id,
            container_id=container_id,
            container=messagepack.message_types.Container(
                container_id=container_id,
                name="RedSolo",
                images=[
                    messagepack.message_types.ContainerImage(
                        image_type=container_type,
                        location=container_loc,
                        created_at=1699389746.8433976,
                        modified_at=1699389746.8433977,
                    )
                ],
            ),
            task_buffer=task_body,
        )
    )

    mock_put = mocker.patch.object(engine.outgoing_q, "put")

    engine.container_type = container_type
    engine.submit(str(task_id), task_message)

    a, _ = mock_put.call_args
    unpacked_msg = InternalTask.unpack(a[0])
    assert unpacked_msg.container_id == container_loc


@pytest.mark.parametrize("task_id", (str(uuid.uuid4()), None))
def test_engine_invalid_result_data(task_id: t.Optional[str]):
    htex = HighThroughputEngine(address="127.0.0.1")
    htex.incoming_q = mock.MagicMock()
    htex.results_passthrough = mock.MagicMock()
    htex.tasks = mock.MagicMock()
    htex.is_alive = True
    htex._engine_bad_state = threading.Event()

    result_message = {"task_id": task_id}
    htex.incoming_q.get.return_value = [dill.dumps(result_message)]

    queue_mgmt_thread = threading.Thread(target=htex._queue_management_worker)
    queue_mgmt_thread.start()

    if task_id:
        try_assert(lambda: htex.results_passthrough.put.called)
        res = htex.results_passthrough.put.call_args[0][0]
        msg = messagepack.unpack(res["message"])
        assert res["task_id"] == task_id
        assert f"{task_id} failed to run" in msg.data
    else:
        try_assert(lambda: htex.incoming_q.get.call_count > 1)
        assert not htex.results_passthrough.put.called

    htex.is_alive = False
    htex._engine_bad_state.set()
    queue_mgmt_thread.join()
