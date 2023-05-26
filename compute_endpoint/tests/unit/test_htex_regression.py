import concurrent.futures
import logging
import multiprocessing
import random
import uuid

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import HighThroughputEngine
from globus_compute_sdk.serialize import ComputeSerializer
from tests.utils import double, ez_pack_function

logger = logging.getLogger(__name__)


@pytest.fixture
def engine():
    ep_id = uuid.uuid4()
    engine = HighThroughputEngine(
        label="HTEXEngine",
        heartbeat_period=1,
        worker_debug=True,
    )
    queue = multiprocessing.Queue()
    engine.start(endpoint_id=ep_id, run_dir="/tmp", results_passthrough=queue)

    yield engine
    engine.shutdown()


@pytest.mark.skip()
def test_engine_submit_internal(engine):
    "Test engine.submit with multiple engines"

    param = random.randint(1, 100)
    serializer = ComputeSerializer()
    task_id = uuid.uuid1()
    future = engine._submit(double, param, task_id=task_id)
    assert isinstance(future, concurrent.futures.Future)

    # The content of the future object is likely junk
    # Do not check if the result makes sense
    assert future.result()
    logger.warning(f"Got result {future.result()}")

    unpacked_result = messagepack.unpack(future.result())
    assert isinstance(unpacked_result, messagepack.message_types.Result)
    assert unpacked_result.task_id == task_id
    logger.warning(f"Result: {unpacked_result}")
    final_result = serializer.deserialize(unpacked_result.data)
    assert final_result == 2 * param


def test_engine_submit(engine):
    q = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, double, (3,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )
    future = engine.submit(task_id, task_message)
    packed_result = future.result()

    # Confirm that the future got the right answer
    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_id

    # Confirm that the same result got back though the queue
    for _i in range(10):
        packed_result_q = q.get(timeout=0.1)
        assert isinstance(
            packed_result_q, bytes
        ), "Expected bytes from the passthrough_q"

        result = messagepack.unpack(packed_result_q)
        # Handle a sneaky EPStatusReport that popped in ahead of the result
        if isinstance(result, messagepack.message_types.EPStatusReport):
            continue

        # At this point the message should be the result
        assert (
            packed_result == packed_result_q
        ), "Result from passthrough_q and future should match"

        assert result.task_id == task_id
        logger.warning(f"Result info : {result}")
        logger.warning(f"Result.data : {result.data}")
        final_result = serializer.deserialize(result.data)
        assert final_result == 6, f"Expected 6, but got: {final_result}"
        break
