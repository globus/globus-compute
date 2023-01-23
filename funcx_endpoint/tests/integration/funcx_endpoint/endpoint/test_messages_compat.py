import pickle
import uuid

from funcx_common.messagepack import unpack
from funcx_common.messagepack.message_types import (
    EPStatusReport as OutgoingEPStatusReport,
)
from funcx_common.messagepack.message_types import Task as OutgoingTask
from funcx_common.messagepack.message_types import TaskTransition
from funcx_common.tasks.constants import ActorName, TaskState

from funcx_endpoint.endpoint.messages_compat import (
    convert_to_internaltask,
    try_convert_to_messagepack,
)
from funcx_endpoint.executors.high_throughput.messages import (
    EPStatusReport as InternalEPStatusReport,
)
from funcx_endpoint.executors.high_throughput.messages import Message as InternalMessage
from funcx_endpoint.executors.high_throughput.messages import Task as InternalTask


def test_ep_status_report_conversion():
    ep_id = uuid.uuid4()
    ep_status_report = {"looking": "good"}
    task_statuses = {
        "1": [
            TaskTransition(
                timestamp=1,
                state=TaskState.EXEC_END,
                actor=ActorName.INTERCHANGE,
            )
        ],
        "2": [
            TaskTransition(
                timestamp=1,
                state=TaskState.EXEC_END,
                actor=ActorName.INTERCHANGE,
            )
        ],
    }

    internal = InternalEPStatusReport(str(ep_id), ep_status_report, task_statuses)
    message = pickle.dumps(internal)

    outgoing = try_convert_to_messagepack(message)
    external = unpack(outgoing)

    assert isinstance(external, OutgoingEPStatusReport)
    assert external.endpoint_id == ep_id
    assert external.ep_status_report == ep_status_report
    assert external.task_statuses == task_statuses


def test_external_task_to_internal_task():
    task_id = uuid.uuid4()
    container_id = uuid.uuid4()
    task_buffer = b"task_buffer"

    external = OutgoingTask(
        task_id=task_id, container_id=container_id, task_buffer=task_buffer
    )

    incoming = convert_to_internaltask(external)
    internal = InternalMessage.unpack(incoming)

    assert isinstance(internal, InternalTask)
    assert internal.task_id == str(task_id)
    assert internal.container_id == str(container_id)
    assert internal.task_buffer == task_buffer


def test_external_task_without_container_id_converts_to_RAW():
    task_id = uuid.uuid4()
    task_buffer = b"task_buffer"

    external = OutgoingTask(task_id=task_id, container_id=None, task_buffer=task_buffer)

    incoming = convert_to_internaltask(external)
    internal = InternalMessage.unpack(incoming)

    assert isinstance(internal, InternalTask)
    assert internal.task_id == str(task_id)
    assert internal.container_id == "RAW"
    assert internal.task_buffer == task_buffer
