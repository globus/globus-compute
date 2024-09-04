import logging
from unittest import mock

from globus_compute_common import messagepack
from globus_compute_endpoint.engines.helper import execute_task

logger = logging.getLogger(__name__)

_MOCK_BASE = "globus_compute_endpoint.engines.helper."


def divide(x, y):
    return x / y


def test_execute_task(endpoint_uuid, serde, task_uuid, ez_pack_task):
    inp, outp = (10, 2), 5

    task_bytes = ez_pack_task(divide, *inp)

    packed_result = execute_task(task_uuid, task_bytes, endpoint_uuid)
    assert isinstance(packed_result, bytes)

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.data
    assert "os" in result.details
    assert "python_version" in result.details
    assert "dill_version" in result.details
    assert "endpoint_id" in result.details
    assert serde.deserialize(result.data) == outp


def test_execute_task_with_exception(endpoint_uuid, task_uuid, ez_pack_task):
    task_bytes = ez_pack_task(divide, 10, 0)

    with mock.patch(f"{_MOCK_BASE}log") as mock_log:
        packed_result = execute_task(task_uuid, task_bytes, endpoint_uuid)

    assert mock_log.exception.called
    a, _k = mock_log.exception.call_args
    assert "while executing user function" in a[0]

    assert isinstance(packed_result, bytes)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.error_details
    assert "python_version" in result.details
    assert "os" in result.details
    assert "dill_version" in result.details
    assert "endpoint_id" in result.details
    assert "ZeroDivisionError" in result.data
