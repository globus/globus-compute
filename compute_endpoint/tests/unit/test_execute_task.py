import logging
import random
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines.helper import execute_task
from tests.utils import divide

logger = logging.getLogger(__name__)

_MOCK_BASE = "globus_compute_endpoint.engines.helper."


@pytest.mark.parametrize("run_dir", ("tmp", None, "$HOME"))
def test_bad_run_dir(endpoint_uuid, task_uuid, run_dir):
    with pytest.raises(RuntimeError):
        execute_task(task_uuid, b"", endpoint_uuid, run_dir=run_dir)


def test_execute_task(endpoint_uuid, serde, task_uuid, ez_pack_task, tmp_path):
    out = random.randint(1, 100_000)
    divisor = random.randint(1, 100_000)

    task_bytes = ez_pack_task(divide, divisor * out, divisor)

    packed_result = execute_task(task_uuid, task_bytes, endpoint_uuid, run_dir=tmp_path)
    assert isinstance(packed_result, bytes)

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.data
    assert "os" in result.details
    assert "python_version" in result.details
    assert "dill_version" in result.details
    assert "endpoint_id" in result.details
    assert serde.deserialize(result.data) == out


def test_execute_task_with_exception(endpoint_uuid, task_uuid, ez_pack_task, tmp_path):
    task_bytes = ez_pack_task(divide, 10, 0)

    with mock.patch(f"{_MOCK_BASE}log") as mock_log:
        packed_result = execute_task(
            task_uuid, task_bytes, endpoint_uuid, run_dir=tmp_path
        )

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
