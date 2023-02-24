from __future__ import annotations

import logging
import pickle
import uuid

from funcx_common.messagepack import Message as OutgoingMessage
from funcx_common.messagepack import pack
from funcx_common.messagepack.message_types import (
    EPStatusReport as OutgoingEPStatusReport,
)
from funcx_common.messagepack.message_types import Result as OutgoingResult
from funcx_common.messagepack.message_types import (
    ResultErrorDetails as OutgoingResultErrorDetails,
)
from funcx_common.messagepack.message_types import Task as OutgoingTask
from funcx_common.messagepack.message_types import TaskTransition

from funcx_endpoint.executors.high_throughput.messages import (
    EPStatusReport as InternalEPStatusReport,
)
from funcx_endpoint.executors.high_throughput.messages import Task as InternalTask

logger = logging.getLogger(__name__)


def try_convert_to_messagepack(message: bytes) -> bytes:
    try:
        unpacked = pickle.loads(message)
    except pickle.UnpicklingError:
        # message isn't pickled; assume that it's already in messagepack format
        return message

    messagepack_msg: OutgoingMessage | None = None

    if isinstance(unpacked, InternalEPStatusReport):
        messagepack_msg = OutgoingEPStatusReport(
            endpoint_id=unpacked._header,
            ep_status_report=unpacked.ep_status,
            task_statuses=unpacked.task_statuses,
        )
    elif isinstance(unpacked, dict):
        kwargs: dict[
            str, str | uuid.UUID | OutgoingResultErrorDetails | list[TaskTransition]
        ] = {
            "task_id": uuid.UUID(unpacked["task_id"]),
        }
        if "task_statuses" in unpacked:
            kwargs["task_statuses"] = unpacked["task_statuses"]
        if "exception" in unpacked:
            kwargs["data"] = unpacked["exception"]
            code, user_message = unpacked.get("error_details", ("Unknown", "Unknown"))
            kwargs["error_details"] = OutgoingResultErrorDetails(
                code=code, user_message=user_message
            )
        else:
            kwargs["data"] = unpacked["data"]

        messagepack_msg = OutgoingResult(**kwargs)

    if messagepack_msg:
        message = pack(messagepack_msg)

    return message


def convert_to_internaltask(message: OutgoingTask, container_type: str | None) -> bytes:
    container_loc = "RAW"
    if message.container:
        for img in message.container.images:
            if img.image_type == container_type:
                container_loc = img.location
                break

    return InternalTask(
        task_id=str(message.task_id),
        container_id=container_loc,
        task_buffer=message.task_buffer,
    ).pack()
