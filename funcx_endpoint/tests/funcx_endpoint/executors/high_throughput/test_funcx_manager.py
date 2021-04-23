from funcx_endpoint.executors.high_throughput.funcx_manager import Manager
from funcx_endpoint.executors.high_throughput.messages import Task
import queue


class TestManager:

    def test_remove_worker_init(self):
        class MockMap:
            def __init__(self):
                self.to_die_count = {"RAW": 0}
        manager = Manager(uid="mock_uid")
        manager.worker_map = MockMap()
        manager.task_queues = {"RAW": queue.Queue()}

        manager.remove_worker_init("RAW")
        task = manager.task_queues["RAW"].get()
        assert isinstance(task, Task)
        assert task.task_id == "KILL"
        assert task.container_id == "RAW"
        assert task.task_buffer.decode('utf-8') == "KILL"
        assert task.raw_buffer.decode('utf-8') == "KILL;RAW;KILL"
