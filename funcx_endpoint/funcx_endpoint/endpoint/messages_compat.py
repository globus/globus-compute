from __future__ import annotations

import pickle

from funcx_common.messagepack import Message, pack
from funcx_common.messagepack.message_types import (
    EPStatusReport as OutgoingEPStatusReport,
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

    if messagepack_msg:
        message = pack(messagepack_msg)

    return message
