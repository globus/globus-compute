import os
import pickle
import uuid
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.executors.high_throughput.messages import Task
from globus_compute_endpoint.executors.high_throughput.worker import Worker
from parsl.app.errors import AppTimeout


def hello_world():
    return "hello world"


def failing_function():
    x = {}
    return x["foo"]  # will fail, but in a "natural" way


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
    with mock.patch(
        "globus_compute_endpoint.executors.high_throughput.worker.zmq.Context"
    ) as mock_context:
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
    task_body = ez_pack_function(test_worker.serializer, hello_world, (), {})

    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    result = test_worker.execute_task(str(task_id), task_message)
    assert isinstance(result, dict)
    assert "exception" not in result
    assert isinstance(result.get("data"), str)

    result_data = test_worker.deserialize(result["data"])
    assert result_data == "hello world"


def test_execute_failing_function(test_worker):
    task_id = uuid.uuid1()
    task_body = ez_pack_function(test_worker.serializer, failing_function, (), {})

    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    result = test_worker.execute_task(str(task_id), task_message)
    assert isinstance(result, dict)
    assert "data" not in result
    assert "exception" in result
    assert isinstance(result.get("exception"), str)

    # error string contains the KeyError which failed
    assert "KeyError" in result["exception"]
    # but it does not contain some of the funcx-constructed calling context
    # a result of traceback traversal done when formatting
    assert "call_user_function" not in result["exception"]

    assert isinstance(result.get("error_details"), tuple)
    assert result["error_details"] == (
        "RemoteExecutionError",
        "An error occurred during the execution of this task",
    )


def test_execute_function_exceeding_result_size_limit(test_worker):
    test_worker.result_size_limit = 0
    task_id = uuid.uuid1()
    task_body = ez_pack_function(test_worker.serializer, hello_world, (), {})

    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    result = test_worker.execute_task(str(task_id), task_message)
    assert isinstance(result, dict)
    assert "data" not in result
    assert "exception" in result
    assert isinstance(result.get("exception"), str)

    # error string contains the error
    assert "MaxResultSizeExceeded" in result["exception"]
    # but it does not contain some of the funcx-constructed calling context
    # a result of traceback traversal done when formatting
    assert "call_user_function" not in result["exception"]

    assert isinstance(result.get("error_details"), tuple)
    assert len(result["error_details"]) == 2
    assert result["error_details"][0] == "MaxResultSizeExceeded"
    assert result["error_details"][1].startswith("remote error: ")


def sleeper(t):
    import time

    time.sleep(t)
    return True


def test_app_timeout(test_worker):
    task_id = uuid.uuid1()
    task_body = ez_pack_function(test_worker.serializer, sleeper, (1,), {})
    task_message = messagepack.pack(
        messagepack.message_types.Task(
            task_id=task_id, container_id=uuid.uuid1(), task_buffer=task_body
        )
    )

    with mock.patch.dict(os.environ, {"GC_TASK_TIMEOUT": "0.1"}):
        with pytest.raises(AppTimeout):
            test_worker.call_user_function(task_message)
