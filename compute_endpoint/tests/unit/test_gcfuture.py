import uuid

import pytest
from globus_compute_endpoint.engines import GCFuture


def test_gcfuture_happy_path():
    tid = uuid.uuid4()
    f = GCFuture(gc_task_id=tid)
    f.executor_task_id = 1
    assert f.gc_task_id == tid
    assert f.executor_task_id == 1


def test_gcfuture_task_id_coerced():
    tid = uuid.uuid4()
    fraw = GCFuture(gc_task_id=tid)
    fstr = GCFuture(gc_task_id=str(tid))
    assert fraw.gc_task_id == fstr.gc_task_id
    assert isinstance(fstr.gc_task_id, uuid.UUID)


@pytest.mark.parametrize("ex_tid", (None, 1, "1"))
def test_gcfuture_repr(ex_tid):
    tid = uuid.uuid4()
    tids = str(tid)
    f = GCFuture(gc_task_id=tid)
    assert f"gc_task_id={tids!r}" in repr(f)
    assert "executor_task_id=None" in repr(f)

    f = GCFuture(gc_task_id=tid, executor_task_id=ex_tid)
    assert f"gc_task_id={tids!r}" in repr(f)
    assert f"executor_task_id={ex_tid!r}" in repr(f)
