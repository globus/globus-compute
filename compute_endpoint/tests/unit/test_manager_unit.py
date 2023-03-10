import time
import uuid
from unittest import mock

from globus_compute_common.tasks import TaskState
from globus_compute_endpoint.executors.high_throughput.manager import Manager
from globus_compute_endpoint.executors.high_throughput.messages import Task


@mock.patch("globus_compute_endpoint.executors.high_throughput.manager.zmq")
class TestManager:
    def test_task_to_worker_status_change(self, randomstring):
        task_type = randomstring()
        task_id = str(uuid.uuid4())
        task = Task(task_id, "RAW", b"")

        mgr = Manager(uid="some_uid", worker_type=task_type)
        mgr.worker_map = mock.Mock()
        mgr.worker_map.get_worker.return_value = "some_work_id"
        mgr.task_queues[task_type].put(task)
        mgr.send_task_to_worker(task_type)

        assert task_id in mgr.task_status_deltas

        tt = mgr.task_status_deltas[task_id][0]
        assert time.time_ns() - tt.timestamp < 2000000000, "Expecting a timestamp"
        assert tt.state == TaskState.RUNNING
