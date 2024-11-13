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
from tests.utils import double, ez_pack_function, try_assert


@pytest.fixture(autouse=True)
def warning_invoked(htex_warns):
    yield


@pytest.fixture
def htex(tmp_path):
    ep_id = uuid.uuid4()
    executor = HighThroughputEngine(
        address="127.0.0.1",
        heartbeat_period=1,
        heartbeat_threshold=2,
        worker_debug=True,
    )
    q = queue.Queue()
    executor.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=q)

    yield executor
    executor.shutdown()


def test_engine_submit_container_location(
    mocker: MockFixture, htex: HighThroughputEngine, serde: ComputeSerializer
):
    engine = htex
    task_id = uuid.uuid4()
    container_id = uuid.uuid4()
    container_type = "singularity"
    container_loc = "/path/to/container"
    task_body = ez_pack_function(serde, double, (10,), {})
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
    engine.submit(str(task_id), task_message, {})

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
