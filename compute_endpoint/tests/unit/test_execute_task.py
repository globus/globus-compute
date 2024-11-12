import logging
import os
import random
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines.helper import _RESULT_SIZE_LIMIT, execute_task
from globus_compute_sdk.errors import MaxResultSizeExceeded
from tests.utils import divide

logger = logging.getLogger(__name__)

_MOCK_BASE = "globus_compute_endpoint.engines.helper."


@pytest.mark.parametrize("run_dir", ("", ".", "./", "../", "tmp", "$HOME"))
def test_bad_run_dir(endpoint_uuid, task_uuid, run_dir):
    with pytest.raises(ValueError):  # not absolute
        execute_task(task_uuid, b"", endpoint_uuid, run_dir=run_dir)

    with pytest.raises(TypeError):  # not anything, allow-any-type language
        execute_task(task_uuid, b"", endpoint_uuid, run_dir=None)


def test_happy_path(serde, task_uuid, ez_pack_task, execute_task_runner):
    out = random.randint(1, 100_000)
    divisor = random.randint(1, 100_000)

    task_bytes = ez_pack_task(divide, divisor * out, divisor)

    packed_result = execute_task_runner(task_bytes)
    assert isinstance(packed_result, bytes)

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.data
    assert result.task_id == task_uuid
    assert "os" in result.details
    assert "python_version" in result.details
    assert "dill_version" in result.details
    assert "endpoint_id" in result.details
    assert serde.deserialize(result.data) == out


def test_sandbox(ez_pack_task, execute_task_runner, task_uuid, tmp_path):
    task_bytes = ez_pack_task(divide, 10, 2)
    packed_result = execute_task_runner(task_bytes, run_in_sandbox=True)
    result = messagepack.unpack(packed_result)
    assert result.task_id == task_uuid
    assert result.error_details is None, "Verify test setup: execution successful"

    exp_dir = tmp_path / str(task_uuid)
    assert os.environ.get("GC_TASK_SANDBOX_DIR") == str(exp_dir), "Share dir w/ func"
    assert os.getcwd() == str(exp_dir), "Expect sandbox dir entered"


def test_nested_run_dir(ez_pack_task, task_uuid, tmp_path):
    task_bytes = ez_pack_task(divide, 10, 2)
    nested_root = tmp_path / "a/"
    nested_path = nested_root / "b/c/d"
    assert not nested_root.exists(), "Verify test setup"

    packed_result = execute_task(task_uuid, task_bytes, run_dir=nested_path)

    result = messagepack.unpack(packed_result)
    assert result.error_details is None, "Verify test setup: execution successful"

    assert nested_path.exists(), "Test namesake"


@pytest.mark.parametrize("size_limit", (128, 256, 1024, 4096, _RESULT_SIZE_LIMIT))
def test_result_size_limit(serde, ez_pack_task, execute_task_runner, size_limit):
    task_bytes = ez_pack_task(divide, 10, 2)
    exp_data = f"{MaxResultSizeExceeded.__name__}({size_limit + 1}, {size_limit})"
    res_data_good = "a" * size_limit
    res_data_bad = "a" * (size_limit + 1)

    with mock.patch(f"{_MOCK_BASE}_call_user_function") as mock_callfn:
        with mock.patch(f"{_MOCK_BASE}log.exception"):  # silence tests
            mock_callfn.return_value = res_data_good
            res_bytes = execute_task_runner(task_bytes, result_size_limit=size_limit)
            result = messagepack.unpack(res_bytes)
            assert result.data == res_data_good

            mock_callfn.return_value = res_data_bad
            res_bytes = execute_task_runner(task_bytes, result_size_limit=size_limit)
            result = messagepack.unpack(res_bytes)
            assert exp_data == result.data
            assert result.error_details.code == "MaxResultSizeExceeded"


def test_default_result_size_limit(ez_pack_task, execute_task_runner):
    task_bytes = ez_pack_task(divide, 10, 2)
    default = _RESULT_SIZE_LIMIT
    exp_data = f"{MaxResultSizeExceeded.__name__}({default + 1}, {default})"
    res_data_good = "a" * default
    res_data_bad = "a" * (default + 1)

    with mock.patch(f"{_MOCK_BASE}_call_user_function") as mock_callfn:
        with mock.patch(f"{_MOCK_BASE}log.exception"):  # silence tests
            mock_callfn.return_value = res_data_good
            res_bytes = execute_task_runner(task_bytes)
            result = messagepack.unpack(res_bytes)
            assert result.data == res_data_good

            mock_callfn.return_value = res_data_bad
            res_bytes = execute_task_runner(task_bytes)
            result = messagepack.unpack(res_bytes)
            assert exp_data == result.data
            assert result.error_details.code == "MaxResultSizeExceeded"


@pytest.mark.parametrize("size_limit", (-5, 0, 1, 65, 127))
def test_invalid_result_size_limit(size_limit):
    with pytest.raises(ValueError) as pyt_e:
        execute_task("test_tid", b"", run_dir="/", result_size_limit=5)
    assert "must be at least" in str(pyt_e.value)


def test_execute_task_with_exception(ez_pack_task, execute_task_runner):
    task_bytes = ez_pack_task(divide, 10, 0)

    with mock.patch(f"{_MOCK_BASE}log") as mock_log:
        packed_result = execute_task_runner(task_bytes)

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
