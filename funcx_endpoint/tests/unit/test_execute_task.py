import logging
import uuid

from funcx_common import messagepack
from tests.utils import ez_pack_function

from funcx.serialize import FuncXSerializer
from funcx_endpoint.executors.execution_helper.helper import execute_task

logger = logging.getLogger(__name__)


def divide(x, y):
    return x / y


def test_execute_task():
    serializer = FuncXSerializer()
    task_id = uuid.uuid1()
    input, output = (10, 2), 5
    task_body = ez_pack_function(serializer, divide, input, {})

    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    packed_result = execute_task(task_message)
    logger.warning(f"[YADU] Got {packed_result=}")
    assert isinstance(packed_result, bytes)

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.data
    assert serializer.deserialize(result.data) == output


def test_execute_task_with_exception():
    serializer = FuncXSerializer()
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

    packed_result = execute_task(task_message)
    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.error_details
    assert "ZeroDivisionError" in result.data
