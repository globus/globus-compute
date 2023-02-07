import logging
import time
import uuid

from funcx_common import messagepack
from funcx_common.messagepack.message_types import Result, TaskTransition
from funcx_common.tasks import ActorName, TaskState

from funcx.errors import MaxResultSizeExceeded
from funcx.serialize import FuncXSerializer
from funcx_endpoint.exception_handling import get_error_string, get_result_error_details
from funcx_endpoint.exceptions import CouldNotExecuteUserTaskError
from funcx_endpoint.executors.high_throughput.messages import Message

log = logging.getLogger(__name__)

serializer = FuncXSerializer()


def execute_task(
    task_id: uuid.UUID, task_body: bytes, result_size_limit: int = 10 * 1024 * 1024
) -> bytes:
    """

    Parameters
    ----------
    task_id: uuid string
    task_body: packed message as bytes
    result_size_limit: result size in bytes

    Returns
    -------
    messagepack packed Result

    """
    log.debug("executing task task_id='%s'", task_id)
    exec_start = TaskTransition(
        timestamp=time.time_ns(), state=TaskState.EXEC_START, actor=ActorName.WORKER
    )

    try:
        result = call_user_function(task_body, result_size_limit=result_size_limit)
    except Exception:
        log.exception("Caught an exception while executing user function")
        code, user_message = get_result_error_details()
        error_details = {"code": code, "user_message": user_message}
        result_message: dict[
            str, str | tuple[str, str] | list[TaskTransition] | dict[str, str]
        ] = dict(
            task_id=task_id,
            data=get_error_string(),
            exception=get_error_string(),
            error_details=error_details,
        )

    else:
        log.debug("Execution completed without exception")
        result_message = dict(task_id=task_id, data=result)

    exec_end = TaskTransition(
        timestamp=time.time_ns(),
        state=TaskState.EXEC_END,
        actor=ActorName.WORKER,
    )

    result_message["task_statuses"] = [exec_start, exec_end]

    log.debug(
        "task %s completed in %d ns",
        task_id,
        (exec_end.timestamp - exec_start.timestamp),
    )

    return messagepack.pack(Result(**result_message))


def call_user_function(
    message: bytes, result_size_limit: int, serializer=serializer
) -> str:
    """Deserialize the buffer and execute the task.

    Returns the result or throws exception.
    """
    # try to unpack it as a messagepack message
    try:
        task = messagepack.unpack(message)
        if not isinstance(task, messagepack.message_types.Task):
            raise CouldNotExecuteUserTaskError(
                f"wrong type of message in worker: {type(task)}"
            )
        task_data = task.task_buffer
    # on parse errors, failover to trying the "legacy" message reading
    except (
        messagepack.InvalidMessageError,
        messagepack.UnrecognizedProtocolVersion,
    ):
        task = Message.unpack(message)
        task_data = task.task_buffer.decode("utf-8")  # type: ignore[attr-defined]

    f, args, kwargs = serializer.unpack_and_deserialize(task_data)
    result_data = f(*args, **kwargs)
    serialized_data = serializer.serialize(result_data)

    if len(serialized_data) > result_size_limit:
        raise MaxResultSizeExceeded(len(serialized_data), result_size_limit)

    return serialized_data
