from globus_compute_endpoint.engines import GCFuture, ThreadPoolEngine
from tests.utils import double


class _Dict(dict):
    def __init__(self, *a, **k):
        self.deleted_items = []
        super().__init__(*a, **k)

    def pop(self, key, *a, **k):
        val = dict.pop(self, key, *a, **k)
        self.deleted_items.append((key, val))
        return val


def test_tasks_cleared_when_complete(ez_pack_task, endpoint_uuid, task_uuid):
    # important for memory that we clean up each task
    eng = ThreadPoolEngine()
    eng.start(endpoint_id=endpoint_uuid)
    eng._task_id_map = _Dict()
    task_bytes = ez_pack_task(double, 1)
    f = GCFuture(task_uuid)

    eng.submit(f, task_bytes, {})

    _ = f.result()  # ensure task is done before test continues
    assert len(eng._task_id_map) == 0
    ex_tid, ex_fut = eng._task_id_map.deleted_items[-1]
    assert ex_tid == f.executor_task_id
    assert ex_fut is f
