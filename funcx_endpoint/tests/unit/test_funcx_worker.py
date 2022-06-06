import pickle

import pytest

from funcx_endpoint.executors.high_throughput.funcx_worker import FuncXWorker
from funcx_endpoint.executors.high_throughput.messages import Task


def test_register_and_kill(mocker):
    mock_context = mocker.patch(
        "funcx_endpoint.executors.high_throughput.funcx_worker.zmq.Context"
    )
    # the worker will receive tasks and send messages on this mock socket
    mock_socket = mocker.Mock()
    mock_context.return_value.socket.return_value = mock_socket
    # send a kill message on the mock socket
    task = Task(task_id="KILL", container_id="RAW", task_buffer="KILL")
    mock_socket.recv_multipart.return_value = (
        pickle.dumps("KILL"),
        pickle.dumps("abc"),
        task.pack(),
    )

    # calling worker.start begins a while loop, where first a REGISTER
    # message is sent out, then the worker receives the KILL task, which
    # triggers a WRKR_DIE message to be sent before the while loop exits
    worker = FuncXWorker("0", "127.0.0.1", 50001)

    # run start() and confirm that it raises a SystemExit because of the kill message
    # which is mocked into place above
    with pytest.raises(SystemExit):
        worker.start()

    # these 2 calls to send_multipart happen in a sequence
    mock_socket.send_multipart.assert_called()
    arglist = mock_socket.send_multipart.call_args_list
    assert len(arglist) == 2, arglist
    for x in arglist:
        assert len(x[0]) == 1, x
        assert len(x[1]) == 0, x
    messages = [x[0][0] for x in arglist]
    assert all(isinstance(m, list) and len(m) == 2 for m in messages), messages
    assert messages[0][0] == b"REGISTER", messages
    assert messages[1][0] == b"WRKR_DIE", messages
