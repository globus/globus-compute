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

    def test_remove_worker_init(self):
        manager = Manager(logdir='./', uid="mock_uid")
        manager.worker_map.to_die_count["RAW"] = 0
        manager.task_queues["RAW"] = queue.Queue()

        manager.remove_worker_init("RAW")
        task = manager.task_queues["RAW"].get()
        assert isinstance(task, Task)
        assert task.task_id == "KILL"
        assert task.task_buffer == "KILL"

    def test_manager_worker(self):
        manager = Manager(logdir='./', uid="mock_uid")
        manager.worker_map.to_die_count["RAW"] = 0
        manager.task_queues["RAW"] = queue.Queue()
        manager.logdir = "./"
        manager.worker_type = 'RAW'

        # Manager::start()
        worker = manager.worker_map.add_worker(worker_id="0",
                                               worker_type=manager.worker_type,
                                               address=manager.address,
                                               debug=logging.DEBUG,
                                               uid=manager.uid,
                                               logdir=manager.logdir,
                                               worker_port=manager.worker_port)
        # The worker process should have been spawned above,
        # and it should have sent out the registration message
        manager.worker_procs.update(worker)
        assert len(manager.worker_procs) == 1

        task_done_count = 0
        reg_info, task_done_count = manager.poll_funcx_task_socket(task_done_count)
        assert reg_info['worker_id'] == '0'
        assert reg_info['worker_type'] == 'RAW'
        assert task_done_count == 0

        # Begin testing removing worker process
        task_type = "RAW"
        manager.remove_worker_init(task_type)
        manager.send_task_to_worker(task_type)

        reg_info, task_done_count = manager.poll_funcx_task_socket(task_done_count)
        assert reg_info is None
        assert len(manager.worker_procs) == 0
        assert task_done_count == 0
