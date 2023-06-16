import logging
import uuid

from globus_compute_common import messagepack
from globus_compute_endpoint.engines.helper import execute_task
from globus_compute_sdk.serialize import ComputeSerializer
from tests.utils import ez_pack_function

logger = logging.getLogger(__name__)


def divide(x, y):
    return x / y


def test_execute_task():
    serializer = ComputeSerializer()
    task_id = uuid.uuid1()
    input, output = (10, 2), 5
    task_body = ez_pack_function(serializer, divide, input, {})

    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    packed_result = execute_task(task_id, task_message)
    assert isinstance(packed_result, bytes)

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.data
    assert serializer.deserialize(result.data) == output


def test_execute_task_with_exception():
    serializer = ComputeSerializer()
    task_id = uuid.uuid1()
    task_body = ez_pack_function(
        serializer,
        divide,
        (
            10,
            0,
        ),
        {},
    )

    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    packed_result = execute_task(task_id, task_message)
    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.error_details
    assert "ZeroDivisionError" in result.data
