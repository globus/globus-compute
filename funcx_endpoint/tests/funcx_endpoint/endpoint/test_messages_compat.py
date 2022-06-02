import pickle
import uuid

from funcx_common.messagepack import unpack
from funcx_common.messagepack.message_types import (
    EPStatusReport as OutgoingEPStatusReport,
)

from funcx_endpoint.endpoint.messages_compat import try_convert_to_messagepack
from funcx_endpoint.executors.high_throughput.messages import (
    EPStatusReport as InternalEPStatusReport,
)


def test_ep_status_report_conversion():
    ep_id = uuid.uuid4()
    ep_status_report = {"looking": "good"}
    task_statuses = {"1": "done", "2": "still working on that one"}

    internal = InternalEPStatusReport(str(ep_id), ep_status_report, task_statuses)
    message = pickle.dumps(internal)

    outgoing = try_convert_to_messagepack(message)
    external = unpack(outgoing)

    assert isinstance(external, OutgoingEPStatusReport)
    assert external.endpoint_id == ep_id
    assert external.ep_status_report == ep_status_report
    assert external.task_statuses == task_statuses
