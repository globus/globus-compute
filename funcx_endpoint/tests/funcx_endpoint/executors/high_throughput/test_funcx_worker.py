from funcx_endpoint.executors.high_throughput.funcx_worker import FuncXWorker
from funcx_endpoint.executors.high_throughput.messages import Task
import os
import pickle


class TestWorker:
    def test_register_and_kill(self, mocker):
        mocker.patch('funcx_endpoint.executors.high_throughput.funcx_worker.sys.exit')

        mock_context = mocker.patch('funcx_endpoint.executors.high_throughput.funcx_worker.zmq.Context')
        mock_socket = mocker.Mock()
        mock_context.return_value.socket.return_value = mock_socket
        task = Task(task_id='KILL',
                    container_id='RAW',
                    task_buffer='KILL')
        mock_socket.recv_multipart.return_value = (pickle.dumps("KILL"), pickle.dumps("abc"), task.pack())

        worker = FuncXWorker('0', '127.0.0.1', 50001, os.getcwd())

        worker.start()

        call1 = [b'REGISTER', pickle.dumps(worker.registration_message())]
        call2 = [b'WRKR_DIE', pickle.dumps(None)]
        mock_socket.send_multipart.assert_any_call(call1)
        mock_socket.send_multipart.assert_any_call(call2)
