import random
import typing as t
import uuid

import pytest
from globus_compute_endpoint.engines import GCFuture


def test_gcfuture_happy_path():
    tid = uuid.uuid4()
    f = GCFuture(tid)
    f.executor_task_id = 1
    assert f.gc_task_id == tid
    assert f.executor_task_id == 1


def test_gcfuture_task_id_coerced():
    tid = uuid.uuid4()
    fraw = GCFuture(tid)
    fstr = GCFuture(str(tid))
    assert fraw.gc_task_id == fstr.gc_task_id
    assert isinstance(fstr.gc_task_id, uuid.UUID)


def test_gcfuture_function_id_coerced():
    tid = uuid.uuid4()
    fraw = GCFuture(tid, function_id=tid)
    fstr = GCFuture(tid, function_id=str(tid))
    fadd = GCFuture(tid)
    fadd.function_id = str(tid)
    assert fraw.function_id == fstr.function_id
    assert fraw.function_id == fadd.function_id
    assert isinstance(fraw.function_id, uuid.UUID)


@pytest.mark.parametrize("tid", (uuid.uuid4(), str(uuid.uuid4())))
@pytest.mark.parametrize("fid", (None, str(uuid.uuid4())))
@pytest.mark.parametrize("bid", (None, 1, "1"))
@pytest.mark.parametrize("jid", (None, 2, "2"))
@pytest.mark.parametrize("ex_tid", (None, 3, "3"))
def test_gcfuture_repr(tid, fid, bid, jid, ex_tid):
    f = GCFuture(
        tid,
        function_id=fid,
        block_id=bid,
        job_id=jid,
        executor_task_id=ex_tid,
    )
    tids = str(tid)

    r = repr(f)
    assert f"{tids!r}" in r

    assert (fid is None and "function_id" not in r) ^ (f"function_id={fid!r}" in r)
    assert (bid is None and "block_id" not in r) ^ (f"block_id={bid!r}" in r)
    assert (jid is None and "job_id" not in r) ^ (f"job_id={jid!r}" in r)
    assert (ex_tid is None and "executor_task_id" not in r) ^ (
        f"executor_task_id={ex_tid!r}" in r
    )


def test_gcfuture_bind():
    def val_change(name: str) -> t.Callable[[GCFuture, t.Any], None]:
        def _(fut: GCFuture, new_val) -> None:
            changed[name] = new_val

        return _

    changed = {}
    f = GCFuture(uuid.uuid4())
    f.bind("block_id", val_change("bid"))
    f.bind("job_id", val_change("jid"))
    f.bind("executor_task_id", val_change("exid"))
    for _ in range(3):
        new_bid = random.randint(0, 1000)
        new_jid = random.randint(2000, 3000)
        new_exid = random.randint(4000, 5000)
        f.block_id = new_bid
        assert changed["bid"] == new_bid
        f.job_id = new_jid
        assert changed["jid"] == new_jid
        f.executor_task_id = new_exid
        assert changed["exid"] == new_exid
    assert "bid" in changed
    assert "jid" in changed
    assert "exid" in changed
