from funcx_endpoint.executors.high_throughput.funcx_manager import Manager
from funcx_endpoint.executors.high_throughput.messages import Task
import queue
import logging
import pickle
import zmq
import os
import shutil
import pytest


class TestManager:

    @pytest.fixture(autouse=True)
    def test_setup_teardown(self):
        os.makedirs(os.path.join(os.getcwd(), 'mock_uid'))
        yield
        shutil.rmtree(os.path.join(os.getcwd(), 'mock_uid'))

    def test_remove_worker_init(self, mocker):
        mocker.patch('funcx_endpoint.executors.high_throughput.funcx_manager.zmq.Context')

        mock_thread = mocker.patch('funcx_endpoint.executors.high_throughput.funcx_manager.threading.Thread')
        mock_thread.return_value = None

        manager = Manager(logdir='./', uid="mock_uid")
        manager.worker_map.to_die_count["RAW"] = 0
        manager.task_queues["RAW"] = queue.Queue()

        manager.remove_worker_init("RAW")
        task = manager.task_queues["RAW"].get()
        assert isinstance(task, Task)
        assert task.task_id == "KILL"
        assert task.task_buffer == "KILL"

    def test_poll_funcx_task_socket(self, mocker):
        mocker.patch('funcx_endpoint.executors.high_throughput.funcx_manager.zmq.Context')

        mock_worker_map = mocker.patch('funcx_endpoint.executors.high_throughput.funcx_manager.WorkerMap')

        mock_thread = mocker.patch('funcx_endpoint.executors.high_throughput.funcx_manager.threading.Thread')
        mock_thread.return_value = None

        manager = Manager(logdir='./', uid="mock_uid")
        manager.task_queues["RAW"] = queue.Queue()
        manager.logdir = "./"
        manager.worker_type = 'RAW'
        manager.worker_procs['0'] = 'proc'

        manager.funcx_task_socket.recv_multipart.return_value = b'0', b'REGISTER', pickle.dumps({'worker_type': 'RAW'})
        manager.poll_funcx_task_socket(test=True)
        mock_worker_map.return_value.register_worker.assert_called_with(b'0', 'RAW')

        manager.funcx_task_socket.recv_multipart.return_value = b'0', b'WRKR_DIE', pickle.dumps(None)
        manager.poll_funcx_task_socket(test=True)
        mock_worker_map.return_value.remove_worker.assert_called_with(b'0')
        assert len(manager.worker_procs) == 0
