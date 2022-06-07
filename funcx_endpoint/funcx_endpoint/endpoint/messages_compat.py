from __future__ import annotations

import pickle
import uuid

from funcx_common.messagepack import Message, pack
from funcx_common.messagepack.message_types import (
    EPStatusReport as OutgoingEPStatusReport,
)
from funcx_common.messagepack.message_types import Result as OutgoingResult
from funcx_common.messagepack.message_types import (
    ResultErrorDetails as OutgoingResultErrorDetails,
)

from funcx_endpoint.executors.high_throughput.messages import (
    EPStatusReport as InternalEPStatusReport,
)


def try_convert_to_messagepack(message: bytes) -> bytes:
    try:
        unpacked = pickle.loads(message)
    except pickle.UnpicklingError:
        # message isn't pickled; assume that it's already in messagepack format
        return message

    messagepack_msg: Message | None = None

    if isinstance(unpacked, InternalEPStatusReport):
        messagepack_msg = OutgoingEPStatusReport(
            endpoint_id=unpacked._header,
            ep_status_report=unpacked.ep_status,
            task_statuses=unpacked.task_statuses,
        )
    elif isinstance(unpacked, dict):
        kwargs = {
            "task_id": uuid.UUID(unpacked["task_id"]),
            "exec_start_ms": unpacked["exec_start_ms"],
            "exec_end_ms": unpacked["exec_end_ms"],
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
