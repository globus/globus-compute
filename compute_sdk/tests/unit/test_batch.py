import random
import uuid

import pytest
from globus_compute_sdk.sdk.batch import Batch
from globus_compute_sdk.sdk.utils.uuid_like import as_optional_uuid


@pytest.mark.parametrize("val", (None, "", 0, uuid.uuid4(), str(uuid.uuid4())))
def test_task_group_id_good(val):
    b = Batch(None)
    assert b.task_group_id is None

    b.task_group_id = val
    assert b.task_group_id == as_optional_uuid(val)
    b.task_group_id = None
    assert b.task_group_id is None


@pytest.mark.parametrize("val", (123, " ", "a", str(uuid.uuid4())[:-1]))
def test_task_group_id_bad(val):
    b = Batch(None)
    assert b.task_group_id is None

    with pytest.raises((ValueError, AttributeError)):
        b.task_group_id = val


def test_add_success():
    b = Batch(None)
    fn_id = uuid.uuid4()
    b.add(fn_id)
    b.add(fn_id, ("asdf",))
    b.add(fn_id, ("asdf",), {"a": 1})


@pytest.mark.parametrize("val", (1234, "asdf", "a", ""))
def test_add_invalid_function_id(val):
    b = Batch(None)
    with pytest.raises((ValueError, AttributeError)):
        b.add(val)


@pytest.mark.parametrize("val", ("a", "asdf", 1234, {}))
def test_add_invalid_args(val):
    b = Batch(None)
    with pytest.raises(TypeError) as pyt_e:
        b.add(uuid.uuid4(), val)

    bad_type = type(val).__name__
    assert "args: expected `tuple`, got" in str(pyt_e.value)
    assert bad_type in str(pyt_e.value)


@pytest.mark.parametrize("val", ("a", "asdf", 1234))
def test_add_invalid_kwargs(val):
    b = Batch(None)
    with pytest.raises(TypeError) as pyt_e:
        b.add(uuid.uuid4(), kwargs=val)

    bad_type = type(val).__name__
    assert "kwargs: expected dict, got" in str(pyt_e.value)
    assert bad_type in str(pyt_e.value)


def test_add_invalid_kwargs_keys():
    invalid_key = 123
    invalid_kwargs = {"a": 1, invalid_key: 2}
    b = Batch(None)
    with pytest.raises(TypeError) as pyt_e:
        b.add(uuid.uuid4(), kwargs=invalid_kwargs)

    bad_type = type(invalid_key).__name__
    assert "kwargs: expected kwargs with `str` keys, got" in str(pyt_e.value)
    assert bad_type in str(pyt_e.value)


@pytest.mark.parametrize("rq", (None, True, False))
@pytest.mark.parametrize("a", (None, ("a",), ("a", 1, "b")))
@pytest.mark.parametrize("k", (None, {"a": 1}, {"a": 1, "b": 123}))
def test_prepare(rq, a, k):
    tg_id_key = "task_group_id"
    b = Batch(None)
    if rq is not None:
        b.request_queue = rq

    r = b.prepare()
    assert r["create_queue"] is (b.request_queue is True)
    assert not r["tasks"]
    assert tg_id_key not in r

    tg = str(uuid.uuid4())
    b.task_group_id = tg
    r = b.prepare()
    assert tg_id_key in r
    assert r[tg_id_key] == tg

    _serde_a = b._serde.serialize(a or ())
    _serde_k = b._serde.serialize(k or {})
    exp_pld = b._serde.pack_buffers([_serde_a, _serde_k])

    fn_id = str(uuid.uuid4())
    b.add(fn_id, a, k)
    r = b.prepare()
    funcs = r["tasks"]
    assert len(funcs) == 1
    assert fn_id in funcs

    fn_tasks = funcs[fn_id]
    assert len(funcs[fn_id]) == 1
    assert all(pld == exp_pld for pld in fn_tasks)

    num_tasks = random.randint(1, 25)
    for _ in range(1, num_tasks):
        b.add(fn_id, a, k)
    r = b.prepare()
    fn_tasks = r["tasks"][fn_id]
    assert len(fn_tasks) == num_tasks
    assert all(pld == exp_pld for pld in fn_tasks)
