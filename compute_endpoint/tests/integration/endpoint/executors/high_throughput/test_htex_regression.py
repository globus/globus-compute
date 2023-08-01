import queue
import random
import uuid

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import HighThroughputEngine
from globus_compute_sdk.serialize import ComputeSerializer
from tests.utils import double, ez_pack_function


@pytest.fixture
def engine(tmp_path):
    ep_id = uuid.uuid4()
    engine = HighThroughputEngine(
        label="HTEXEngine",
        heartbeat_period=1,
        worker_debug=True,
    )
    results_queue = queue.Queue()
    engine.start(endpoint_id=ep_id, run_dir=tmp_path, results_passthrough=results_queue)

    yield engine
    engine.shutdown()


def test_engine_submit(engine):
    q = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_arg = random.randint(1, 1000)
    task_body = ez_pack_function(serializer, double, (task_arg,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )
    future = engine.submit(str(task_id), task_message)
    packed_result = future.result()

    # Confirm that the future got the right answer
    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_id

    # Confirm that the same result got back though the queue
    for _i in range(10):
        q_msg = q.get(timeout=5)
        assert isinstance(q_msg, dict)

        packed_result_q = q_msg["message"]
        result = messagepack.unpack(packed_result_q)
        # Handle a sneaky EPStatusReport that popped in ahead of the result
        if isinstance(result, messagepack.message_types.EPStatusReport):
            continue

        # At this point the message should be the result
        assert (
            packed_result == packed_result_q
        ), "Result from passthrough_q and future should match"

        assert result.task_id == task_id
        final_result = serializer.deserialize(result.data)
        expected = task_arg * 2
        assert final_result == expected, f"Expected {expected}, but got: {final_result}"
        break
