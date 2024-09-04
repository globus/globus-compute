import queue
import random
import uuid

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import HighThroughputEngine
from tests.utils import double


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


def test_engine_submit(engine, serde, task_uuid, ez_pack_task):
    q = engine.results_passthrough
    task_arg = random.randint(1, 1000)
    task_bytes = ez_pack_task(double, task_arg)
    resource_spec = {}
    future = engine.submit(
        str(task_uuid), task_bytes, resource_specification=resource_spec
    )
    packed_result = future.result()

    # Confirm that the future got the right answer
    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_uuid

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

        assert result.task_id == task_uuid
        final_result = serde.deserialize(result.data)
        expected = task_arg * 2
        assert final_result == expected, f"Expected {expected}, but got: {final_result}"
        break
