import logging
import time
import typing as t
import uuid

from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import Result, Task, TaskTransition
from globus_compute_common.tasks import ActorName, TaskState
from globus_compute_endpoint.exception_handling import (
    get_error_string,
    get_result_error_details,
)
from globus_compute_endpoint.exceptions import CouldNotExecuteUserTaskError
from globus_compute_endpoint.executors.high_throughput.messages import Message
from globus_compute_sdk.errors import MaxResultSizeExceeded
from globus_compute_sdk.serialize import ComputeSerializer

log = logging.getLogger(__name__)

serializer = ComputeSerializer()


def execute_task(task_body: bytes, result_size_limit: int = 10 * 1024 * 1024) -> bytes:
    """Execute task is designed to enable any executor to execute a Task payload
    and return a Result payload, where the payload follows the globus-compute protocols
    This method is placed here to make serialization easy for executor classes
    Parameters
    ----------
    task_id: uuid string
    task_body: packed message as bytes
    result_size_limit: result size in bytes
    Returns
    -------
    messagepack packed Result
    """
    exec_start = TaskTransition(
        timestamp=time.time_ns(), state=TaskState.EXEC_START, actor=ActorName.WORKER
    )

    result_message: dict[
        str,
        t.Union[uuid.UUID, str, tuple[str, str], list[TaskTransition], dict[str, str]],
    ] = {}

    try:
        task, task_buffer = _unpack_messagebody(task_body)
        log.debug("executing task task_id='%s'", task.task_id)
        result = _call_user_function(task_buffer, result_size_limit=result_size_limit)
        log.debug("Execution completed without exception")
        result_message = dict(task_id=task.task_id, data=result)

    except Exception:
        log.exception("Caught an exception while executing user function")
        code, user_message = get_result_error_details()
        error_details = {"code": code, "user_message": user_message}
        result_message = dict(
            task_id=task.task_id,
            data=get_error_string(),
            exception=get_error_string(),
            error_details=error_details,
        )

    exec_end = TaskTransition(
        timestamp=time.time_ns(),
        state=TaskState.EXEC_END,
        actor=ActorName.WORKER,
    )

    result_message["task_statuses"] = [exec_start, exec_end]

    log.debug(
        "task %s completed in %d ns",
        task.task_id,
        (exec_end.timestamp - exec_start.timestamp),
    )

    return messagepack.pack(Result(**result_message))


def _unpack_messagebody(message: bytes) -> t.Tuple[Task, str]:
    """Unpack messagebody as a messagepack message with
    some legacy handling
    Parameters
    ----------
    message: messagepack'ed message body
    Returns
    -------
    tuple(task, task_buffer)
    """
    try:
        task = messagepack.unpack(message)
        if not isinstance(task, messagepack.message_types.Task):
            raise CouldNotExecuteUserTaskError(
                f"wrong type of message in worker: {type(task)}"
            )
        task_buffer = task.task_buffer
    # on parse errors, failover to trying the "legacy" message reading
    except (
        messagepack.InvalidMessageError,
        messagepack.UnrecognizedProtocolVersion,
    ):
        task = Message.unpack(message)
        assert isinstance(task, Task)
        task_buffer = task.task_buffer.decode("utf-8")  # type: ignore[attr-defined]
    return task, task_buffer


def _call_user_function(
    task_buffer: str, result_size_limit: int, serializer=serializer
) -> str:
    """Deserialize the buffer and execute the task.
    Parameters
    ----------
    task_buffer: serialized buffer of (fn, args, kwargs)
    result_size_limit: size limit in bytes for results
    serializer: serializer for the buffers
    Returns
    -------
    Returns serialized result or throws exception.
    """
    f, args, kwargs = serializer.unpack_and_deserialize(task_buffer)
    result_data = f(*args, **kwargs)
    serialized_data = serializer.serialize(result_data)

    if len(serialized_data) > result_size_limit:
        raise MaxResultSizeExceeded(len(serialized_data), result_size_limit)

    return serialized_data
