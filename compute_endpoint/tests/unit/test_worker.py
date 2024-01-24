import os
import pickle
import uuid
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines.helper import execute_task
from globus_compute_endpoint.engines.high_throughput.messages import Task
from globus_compute_endpoint.engines.high_throughput.worker import Worker

_MOCK_BASE = "globus_compute_endpoint.engines.high_throughput.worker."


def hello_world():
    return "hello world"


def failing_function():
    x = {}
    return x["foo"]  # will fail, but in a "natural" way


def large_result(size):
    return bytearray(size)


def ez_pack_function(serializer, func, args, kwargs):
    serialized_func = serializer.serialize(func)
    serialized_args = serializer.serialize(args)
    serialized_kwargs = serializer.serialize(kwargs)
    return serializer.pack_buffers(
        [serialized_func, serialized_args, serialized_kwargs]
    )


@pytest.fixture(autouse=True)
def reset_signals_auto(reset_signals):
    yield


@pytest.fixture
def test_worker():
    with mock.patch(f"{_MOCK_BASE}zmq.Context") as mock_context:
        # the worker will receive tasks and send messages on this mock socket
        mock_socket = mock.Mock()
        mock_context.return_value.socket.return_value = mock_socket
        yield Worker("0", "127.0.0.1", 50001)


def test_register_and_kill(test_worker):
    # send a kill message on the mock socket
    task = Task(task_id="KILL", container_id="RAW", task_buffer="KILL")
    test_worker.task_socket.recv_multipart.return_value = (
        pickle.dumps("KILL"),
        pickle.dumps("abc"),
        task.pack(),
    )

    # calling worker.start begins a while loop, where first a REGISTER
    # message is sent out, then the worker receives the KILL task, which
    # triggers a WRKR_DIE message to be sent before the while loop exits

    # confirm that it raises a SystemExit because of the kill message
    with pytest.raises(SystemExit):
        test_worker.start()

    # these 2 calls to send_multipart happen in a sequence
    test_worker.task_socket.send_multipart.assert_called()
    arglist = test_worker.task_socket.send_multipart.call_args_list
    assert len(arglist) == 2, arglist
    for x in arglist:
        assert len(x[0]) == 1, x
        assert len(x[1]) == 0, x
    messages = [x[0][0] for x in arglist]
    assert all(isinstance(m, list) and len(m) == 2 for m in messages), messages
    assert messages[0][0] == b"REGISTER", messages
    assert messages[1][0] == b"WRKR_DIE", messages


def test_execute_hello_world(test_worker):
    task_id = uuid.uuid1()
    task_body = test_worker.serializer.serialize((hello_world, (), {}))
    internal_task = Task(task_id, "RAW", task_body)
    payload = internal_task.pack()

    result = test_worker._worker_execute_task(str(task_id), payload)
    assert isinstance(result, dict)
    assert "exception" not in result
    assert isinstance(result.get("data"), str)

    assert result["data"] == "hello world"


def test_execute_failing_function(test_worker):
    task_id = uuid.uuid1()
    task_body = test_worker.serializer.serialize((failing_function, (), {}))
    task_message = Task(task_id, "RAW", task_body).pack()

    with mock.patch(f"{_MOCK_BASE}log") as mock_log:
        result = test_worker._worker_execute_task(str(task_id), task_message)
    assert isinstance(result, dict)
    assert "data" in result

    result = messagepack.unpack(result["data"])
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_id

    a, _k = mock_log.exception.call_args
    assert "Failed to execute task" in a[0]

    # error string contains the KeyError which failed
    assert "KeyError" in result.data
    assert result.is_error is True

    assert isinstance(
        result.error_details, messagepack.message_types.ResultErrorDetails
    )
    assert result.error_details.code == "RemoteExecutionError"
    assert (
        result.error_details.user_message
        == "An error occurred during the execution of this task"
    )


def test_execute_function_exceeding_result_size_limit(test_worker):
    return_size = 10

    task_id = uuid.uuid1()
    ep_id = uuid.uuid1()

    payload = ez_pack_function(test_worker.serializer, large_result, (return_size,), {})
    task_body = messagepack.pack(
        messagepack.message_types.Task(task_id=task_id, task_buffer=payload)
    )

    with mock.patch("globus_compute_endpoint.engines.helper.log") as mock_log:
        s_result = execute_task(
            task_id, task_body, ep_id, result_size_limit=return_size - 2
        )
    result = messagepack.unpack(s_result)

    assert isinstance(result, messagepack.message_types.Result)
    assert result.error_details
    assert result.task_id == task_id
    assert result.error_details
    assert result.error_details.code == "MaxResultSizeExceeded"
    assert mock_log.exception.called


def sleeper(t):
    import time

    now = start = time.monotonic()
    while now - start < t:
        time.sleep(0.0001)
        now = time.monotonic()
    return True


def test_app_timeout(test_worker):
    task_id = uuid.uuid1()
    ep_id = uuid.uuid1()
    task_body = ez_pack_function(test_worker.serializer, sleeper, (1,), {})
    task_body = messagepack.pack(
        messagepack.message_types.Task(task_id=task_id, task_buffer=task_body)
    )

    with mock.patch("globus_compute_endpoint.engines.helper.log") as mock_log:
        with mock.patch.dict(os.environ, {"GC_TASK_TIMEOUT": "0.01"}):
            packed_result = execute_task(task_id, task_body, ep_id)

    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_id
    assert "AppTimeout" in result.data
    assert mock_log.exception.called
