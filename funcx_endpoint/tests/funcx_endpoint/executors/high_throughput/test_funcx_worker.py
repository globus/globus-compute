import os
import pickle

from funcx_endpoint.executors.high_throughput.funcx_worker import FuncXWorker
from funcx_endpoint.executors.high_throughput.messages import Task


class TestWorker:
    def test_register_and_kill(self, mocker):
        # we need to mock sys.exit here so that the worker while loop
        # can exit without the test being killed
        mocker.patch("funcx_endpoint.executors.high_throughput.funcx_worker.sys.exit")

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
        worker = FuncXWorker("0", "127.0.0.1", 50001, os.getcwd())
        worker.start()

        # these 2 calls to send_multipart happen in a sequence
        call1 = mocker.call([b"REGISTER", pickle.dumps(worker.registration_message())])
        call2 = mocker.call([b"WRKR_DIE", pickle.dumps(None)])
        mock_socket.send_multipart.assert_has_calls([call1, call2])
