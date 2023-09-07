from __future__ import annotations

import logging
import pickle
import uuid

from globus_compute_common.messagepack import Message as OutgoingMessage
from globus_compute_common.messagepack import pack
from globus_compute_common.messagepack.message_types import (
    EPStatusReport as OutgoingEPStatusReport,
)
from globus_compute_common.messagepack.message_types import Result as OutgoingResult
from globus_compute_common.messagepack.message_types import (
    ResultErrorDetails as OutgoingResultErrorDetails,
)
from globus_compute_common.messagepack.message_types import Task as OutgoingTask
from globus_compute_common.messagepack.message_types import TaskTransition
from globus_compute_endpoint.engines.high_throughput.messages import (
    EPStatusReport as InternalEPStatusReport,
)
from globus_compute_endpoint.engines.high_throughput.messages import (
    Task as InternalTask,
)

logger = logging.getLogger(__name__)


def convert_ep_status_report(
    internal: InternalEPStatusReport,
) -> OutgoingEPStatusReport:
    messagepack_msg = OutgoingEPStatusReport(
        endpoint_id=internal._header,
        global_state=internal.global_state,
        task_statuses=internal.task_statuses,
    )
    return messagepack_msg


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
            global_state=unpacked.global_state,
            task_statuses=unpacked.task_statuses,
        )
    elif isinstance(unpacked, dict):
        kwargs: dict[
            str, str | uuid.UUID | OutgoingResultErrorDetails | list[TaskTransition]
        ] = {
            "task_id": uuid.UUID(unpacked["task_id"]),
        }
        if "details" in unpacked:
            kwargs["details"] = unpacked["details"]
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
