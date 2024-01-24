import logging
import uuid
from unittest import mock

from globus_compute_common import messagepack
from globus_compute_endpoint.engines.helper import execute_task
from globus_compute_sdk.serialize import ComputeSerializer
from tests.utils import ez_pack_function

logger = logging.getLogger(__name__)

_MOCK_BASE = "globus_compute_endpoint.engines.helper."


def divide(x, y):
    return x / y


def test_execute_task():
    serializer = ComputeSerializer()
    ep_id = uuid.uuid1()
    task_id = uuid.uuid1()
    input, output = (10, 2), 5
    task_body = ez_pack_function(serializer, divide, input, {})

    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    packed_result = execute_task(task_id, task_message, ep_id)
    assert isinstance(packed_result, bytes)

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.data
    assert "os" in result.details
    assert "python_version" in result.details
    assert "dill_version" in result.details
    assert "endpoint_id" in result.details
    assert serializer.deserialize(result.data) == output


def test_execute_task_with_exception():
    serializer = ComputeSerializer()
    ep_id = uuid.uuid1()
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

    with mock.patch(f"{_MOCK_BASE}log") as mock_log:
        packed_result = execute_task(task_id, task_message, ep_id)

    assert mock_log.exception.called
    a, _k = mock_log.exception.call_args
    assert "while executing user function" in a[0]

    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.error_details
    assert "python_version" in result.details
    assert "os" in result.details
    assert "dill_version" in result.details
    assert "endpoint_id" in result.details
    assert "ZeroDivisionError" in result.data
