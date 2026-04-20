import logging
import os
import random
from itertools import chain
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import TaskCancel
from globus_compute_endpoint.engines.helper import _RESULT_SIZE_LIMIT, execute_task
from globus_compute_sdk.errors import MaxResultSizeExceeded
from globus_compute_sdk.serialize import (
    CombinedCode,
    ComputeSerializer,
    JSONData,
    SerializationStrategy,
)
from globus_compute_sdk.serialize.concretes import SELECTABLE_CODE_STRATEGIES
from tests.utils import create_task_packer, divide

logger = logging.getLogger(__name__)

_MOCK_BASE = "globus_compute_endpoint.engines.helper."


@pytest.fixture
def mock_log():
    with mock.patch(f"{_MOCK_BASE}log") as m:
        yield m


@pytest.fixture
def task_10_2(ez_pack_task):
    return ez_pack_task(divide, 10, 2)


@pytest.fixture
def unknown_strategy():
    class UnknownStrategy(SerializationStrategy):
        identifier = "aa\n"
        for_code = False

        def serialize(self, data):
            pass

        def deserialize(self, payload):
            pass

    yield UnknownStrategy

    SerializationStrategy._CACHE.pop(UnknownStrategy.identifier)


def sleeper(t: float):
    import time

    now = start = time.monotonic()
    while now - start < t:
        time.sleep(0.0001)
        now = time.monotonic()
    return True


@pytest.mark.parametrize("run_dir", ("", ".", "./", "../", "tmp", "$HOME"))
def test_bad_run_dir(endpoint_uuid, task_uuid, run_dir):
    with pytest.raises(ValueError):  # not absolute
        execute_task(task_uuid, b"", endpoint_uuid, run_dir=run_dir)

    with pytest.raises(TypeError):  # not anything, allow-any-type language
        execute_task(task_uuid, b"", endpoint_uuid, run_dir=None)


def test_happy_path(serde, mock_log, task_uuid, ez_pack_task, execute_task_runner):
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

    log_recs = []
    for a, _ in chain(mock_log.info.call_args_list, mock_log.debug.call_args_list):
        fmt, *a = a
        log_recs.append(a and fmt % tuple(a) or fmt)
    log_msgs = "\n".join(log_recs)

    assert "Preparing to execute" in log_msgs, "Expect log clue of progress"
    assert "Unpacking" in log_msgs, "Expect log clue of progress"
    assert "Deserializing function" in log_msgs, "Expect log to clue of progress"
    assert "Invoking task" in log_msgs, "Expect log clue of progress"
    assert f"func name: {divide.__name__}" in log_msgs
    assert "Task function complete" in log_msgs, "Expect log clue of progress"
    assert "Execution completed" in log_msgs, "Expect log clue of progress"
    assert "Task processing completed in" in log_msgs, "Expect log clue of progress"
    assert len(log_recs) == 7, "Time to update test?"
    assert log_msgs.count(str(task_uuid)) == len(log_recs), "Expect id always prefixed"


def test_sandbox(task_10_2, execute_task_runner, task_uuid, tmp_path):
    packed_result = execute_task_runner(task_10_2, run_in_sandbox=True)
    result = messagepack.unpack(packed_result)
    assert result.task_id == task_uuid
    assert result.error_details is None, "Verify test setup: execution successful"

    exp_dir = tmp_path / str(task_uuid)
    assert os.environ.get("GC_TASK_SANDBOX_DIR") == str(exp_dir), "Share dir w/ func"
    assert os.getcwd() == str(exp_dir), "Expect sandbox dir entered"


def test_nested_run_dir(task_10_2, task_uuid, tmp_path):
    nested_root = tmp_path / "a/"
    nested_path = nested_root / "b/c/d"
    assert not nested_root.exists(), "Verify test setup"

    packed_result = execute_task(task_10_2, task_uuid, run_dir=nested_path)

    result = messagepack.unpack(packed_result)
    assert result.error_details is None, "Verify test setup: execution successful"

    assert nested_path.exists(), "Test namesake"


@pytest.mark.parametrize("size_limit", (128, 256, 1024, 4096, _RESULT_SIZE_LIMIT))
def test_result_size_limit(serde, task_10_2, execute_task_runner, size_limit):
    exp_data = f"{MaxResultSizeExceeded.__name__}({size_limit + 1}, {size_limit})"
    res_data_good = "a" * size_limit
    res_data_bad = "a" * (size_limit + 1)

    with mock.patch(f"{_MOCK_BASE}ComputeSerializer.serialize_from_list") as mock_ser:
        with mock.patch(f"{_MOCK_BASE}log.exception"):  # silence tests
            mock_ser.return_value = res_data_good
            res_bytes = execute_task_runner(task_10_2, result_size_limit=size_limit)
            result = messagepack.unpack(res_bytes)
            assert result.data == res_data_good

            mock_ser.return_value = res_data_bad
            res_bytes = execute_task_runner(task_10_2, result_size_limit=size_limit)
            result = messagepack.unpack(res_bytes)
            assert exp_data == result.data
            assert result.error_details.code == "MaxResultSizeExceeded"


def test_default_result_size_limit(task_10_2, execute_task_runner):
    default = _RESULT_SIZE_LIMIT
    exp_data = f"{MaxResultSizeExceeded.__name__}({default + 1}, {default})"
    res_data_good = "a" * default
    res_data_bad = "a" * (default + 1)

    with mock.patch(f"{_MOCK_BASE}ComputeSerializer.serialize_from_list") as mock_ser:
        with mock.patch(f"{_MOCK_BASE}log.exception"):  # silence tests
            mock_ser.return_value = res_data_good
            res_bytes = execute_task_runner(task_10_2)
            result = messagepack.unpack(res_bytes)
            assert result.data == res_data_good

            mock_ser.return_value = res_data_bad
            res_bytes = execute_task_runner(task_10_2)
            result = messagepack.unpack(res_bytes)
            assert exp_data == result.data
            assert result.error_details.code == "MaxResultSizeExceeded"


@pytest.mark.parametrize("size_limit", (-5, 0, 1, 65, 127))
def test_invalid_result_size_limit(size_limit):
    with pytest.raises(ValueError) as pyt_e:
        execute_task("test_tid", b"", run_dir="/", result_size_limit=5)
    assert "must be at least" in str(pyt_e.value)


def test_task_excepts(ez_pack_task, execute_task_runner, mock_log):
    task_bytes = ez_pack_task(divide, 10, 0)

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


def test_times_out(execute_task_runner, task_uuid, ez_pack_task):
    task_bytes = ez_pack_task(sleeper, 1)

    with mock.patch("globus_compute_endpoint.engines.helper.log") as helper_log:
        with mock.patch.dict(os.environ, {"GC_TASK_TIMEOUT": "0.001"}):
            packed_result = execute_task_runner(task_bytes)

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_uuid
    assert "AppTimeout" in result.data
    assert helper_log.exception.called


@pytest.mark.parametrize("ser", SELECTABLE_CODE_STRATEGIES)
@pytest.mark.parametrize("des", SELECTABLE_CODE_STRATEGIES)
def test_deserializers_enforced(mock_log, execute_task_runner, task_uuid, ser, des):
    if ser is des:  # Testing the *restriction* / "unhappy path"
        return

    cser = ComputeSerializer(ser())
    pack_task = create_task_packer(cser, task_uuid)

    task_bytes = pack_task(divide, 10, 2)

    packed_result = execute_task_runner(
        task_bytes,
        task_deserializers=[
            f"{des.__module__}.{des.__qualname__}",
            "globus_compute_sdk.serialize.JSONData",
        ],
    )

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_uuid
    assert result.error_details.code == "RemoteExecutionError"
    assert f"{type(cser.code_serializer).__name__} disabled by" in result.data


def test_result_serializers(task_10_2, execute_task_runner, task_uuid):
    packed_result = execute_task_runner(
        task_10_2, result_serializers=["globus_compute_sdk.serialize.JSONData"]
    )

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_uuid
    assert result.error_details is None

    ser_strat = JSONData()
    res_data = ser_strat.deserialize(result.data)

    assert res_data == 5.0


def test_bad_result_serializer_list(
    task_10_2, execute_task_runner, task_uuid, unknown_strategy
):
    packed_result = execute_task_runner(
        task_10_2,
        result_serializers=["not a strategy", unknown_strategy, CombinedCode],
    )

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_uuid

    exc_str = result.data

    assert "All strategies failed." in exc_str

    assert "Strategy failures: (3 sub-exceptions)" in exc_str
    assert "Invalid strategy-like 'not a strategy'." in exc_str
    assert "UnknownStrategy is not a known serialization strategy." in exc_str
    assert (
        "CombinedCode is a code serialization strategy, expected a data strategy."
        in exc_str
    )


def test_not_a_task_handled_gracefully(tmp_path, mock_log, task_uuid):
    task_bytes = messagepack.pack(TaskCancel(task_id=task_uuid))
    raw = execute_task(task_bytes, task_uuid, run_dir=tmp_path)
    res = messagepack.unpack(raw)

    assert res.task_id == task_uuid, "Sanity check"
    assert "Non Task-type message" in res.data
    assert TaskCancel.__name__ in res.data
    assert f"(from: [{len(task_bytes)} bytes]" in res.data, "Expect size hint"
    assert f"0x{task_bytes[:8].hex()}..." in res.data, "Ellipsis; don't explode exc"
