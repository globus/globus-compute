from __future__ import annotations

import logging
import pickle
import uuid

from funcx_common.messagepack import InvalidMessageError
from funcx_common.messagepack import Message as OutgoingMessage
from funcx_common.messagepack import pack, unpack
from funcx_common.messagepack.message_types import (
    EPStatusReport as OutgoingEPStatusReport,
)
from funcx_common.messagepack.message_types import Result as OutgoingResult
from funcx_common.messagepack.message_types import (
    ResultErrorDetails as OutgoingResultErrorDetails,
)
from funcx_common.messagepack.message_types import Task as OutgoingTask

from funcx_endpoint.executors.high_throughput.messages import (
    EPStatusReport as InternalEPStatusReport,
)
from funcx_endpoint.executors.high_throughput.messages import Message as InternalMessage
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
        # exec_start|end_ms is not currently being used in prod and also
        # may not be present
        kwargs = {
            "task_id": uuid.UUID(unpacked["task_id"]),
            "exec_start_ms": unpacked.get("exec_start_ms", 0),
            "exec_end_ms": unpacked.get("exec_end_ms", 0),
        }
        if "exception" in unpacked:
            kwargs["data"] = unpacked["exception"]
            code, user_message = unpacked["error_details"]
            kwargs["error_details"] = OutgoingResultErrorDetails(
                code=code, user_message=user_message
            )
        else:
            kwargs["data"] = unpacked["data"]

        messagepack_msg = OutgoingResult(**kwargs)

    if messagepack_msg:
        message = pack(messagepack_msg)

    return message


def try_convert_from_messagepack(message: bytes) -> bytes:
    try:
        unpacked = unpack(message)
    except InvalidMessageError:
        # message isn't in messagepack form,
        # assume it's already in internal message form
        return message

    internal_message: InternalMessage | None = None

    if isinstance(unpacked, OutgoingTask):
        container_id = "RAW" if unpacked.container_id is None else unpacked.container_id
        internal_message = InternalTask(
            task_id=str(unpacked.task_id),
            container_id=str(container_id),
            task_buffer=unpacked.task_buffer,
        )

    if internal_message:
        message = internal_message.pack()

    return message
