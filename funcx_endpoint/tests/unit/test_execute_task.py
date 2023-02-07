import logging
import uuid

from funcx_common import messagepack

from funcx.serialize import FuncXSerializer
from funcx_endpoint.executors.execution_helper.helper import execute_task

logger = logging.getLogger(__name__)


def ez_pack_function(serializer, func, args, kwargs):
    serialized_func = serializer.serialize(func)
    serialized_args = serializer.serialize(args)
    serialized_kwargs = serializer.serialize(kwargs)
    return serializer.pack_buffers(
        [serialized_func, serialized_args, serialized_kwargs]
    )


def divide(x, y):
    return x / y


def test_execute_task():
    task_id = uuid.uuid1()
    serializer = FuncXSerializer()
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
    assert result.task_id == task_id
    assert result.data
    assert serializer.deserialize(result.data) == output


def test_execute_task_with_exception():
    task_id = uuid.uuid1()
    serializer = FuncXSerializer()

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
    assert result.task_id == task_id
    assert result.error_details
    assert "ZeroDivisionError" in result.data
