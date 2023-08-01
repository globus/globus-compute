import pickle
import uuid

import pytest
from globus_compute_common.messagepack import unpack
from globus_compute_common.messagepack.message_types import Container, ContainerImage
from globus_compute_common.messagepack.message_types import (
    EPStatusReport as OutgoingEPStatusReport,
)
from globus_compute_common.messagepack.message_types import Task as OutgoingTask
from globus_compute_common.messagepack.message_types import TaskTransition
from globus_compute_common.tasks.constants import ActorName, TaskState
from globus_compute_endpoint.endpoint.messages_compat import (
    convert_to_internaltask,
    try_convert_to_messagepack,
)
from globus_compute_endpoint.engines.high_throughput.messages import (
    EPStatusReport as InternalEPStatusReport,
)
from globus_compute_endpoint.engines.high_throughput.messages import (
    Message as InternalMessage,
)
from globus_compute_endpoint.engines.high_throughput.messages import (
    Task as InternalTask,
)


def test_ep_status_report_conversion():
    ep_id = uuid.uuid4()
    global_state = {"looking": "good"}
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

    internal = InternalEPStatusReport(str(ep_id), global_state, task_statuses)
    message = pickle.dumps(internal)

    outgoing = try_convert_to_messagepack(message)
    external = unpack(outgoing)

    assert isinstance(external, OutgoingEPStatusReport)
    assert external.endpoint_id == ep_id
    assert external.global_state == global_state
    assert external.task_statuses == task_statuses


def test_external_task_to_internal_task(randomstring):
    task_id = uuid.uuid4()
    task_buffer = b"task_buffer"
    container_type = randomstring()
    location = randomstring()

    external = OutgoingTask(
        task_id=task_id,
        container=Container(
            container_id=uuid.uuid4(),
            name="",
            images=[
                ContainerImage(
                    image_type=container_type,
                    location=location,
                    created_at=0,
                    modified_at=0,
                )
            ],
        ),
        task_buffer=task_buffer,
    )

    incoming = convert_to_internaltask(external, container_type)
    internal = InternalMessage.unpack(incoming)

    assert isinstance(internal, InternalTask)
    assert internal.task_id == str(task_id)
    assert internal.container_id == location
    assert internal.task_buffer == task_buffer


def test_external_task_without_container_id_converts_to_RAW():
    task_id = uuid.uuid4()
    task_buffer = b"task_buffer"

    external = OutgoingTask(task_id=task_id, container_id=None, task_buffer=task_buffer)

    incoming = convert_to_internaltask(external, None)
    internal = InternalMessage.unpack(incoming)

    assert isinstance(internal, InternalTask)
    assert internal.task_id == str(task_id)
    assert internal.container_id == "RAW"
    assert internal.task_buffer == task_buffer


@pytest.mark.parametrize(
    "packed_result",
    [
        [
            (
                b'\x01{"message_type":"result","data":{"task_id":'
                b'"1aa3202c-336d-4c43-9a6e-98711add151d","data":"abc 123",'
                b'"error_details":{"code":"err_code","user_message":"msg"},'
                b'"task_statuses":[]}}'
            ),
            "result",
            "1aa3202c-336d-4c43-9a6e-98711add151d",
            "abc 123",
            "err_code",
        ],
    ],
)
def test_unpack_result_without_details(packed_result):
    raw, result, task_id, data, err = packed_result
    unpacked = unpack(raw)
    assert unpacked.message_type == result
    assert isinstance(unpacked.task_id, uuid.UUID)
    assert str(unpacked.task_id) == task_id
    assert unpacked.data == data
    assert unpacked.error_details.code == err
