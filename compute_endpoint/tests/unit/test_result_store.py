import random

import pytest
from globus_compute_endpoint.endpoint.result_store import ResultStore


@pytest.fixture()
def store(fs) -> ResultStore:
    some_dir = "some/dir"
    fs.create_dir(some_dir)
    yield ResultStore(some_dir)


def test_store_setitem(store, randomstring):
    pld = randomstring().encode()
    key = randomstring()
    assert key not in store

    store[key] = pld
    assert key in store


def test_store_getitem(store, randomstring):
    pld = randomstring().encode()
    key = randomstring()

    store[key] = pld
    assert pld == store[key]


def test_store_del(store, randomstring):
    pld = randomstring().encode()
    key = randomstring()
    store[key] = pld

    assert key in store
    del store[key]
    assert key not in store

    with pytest.raises(FileNotFoundError):
        del store[key]


def test_store_discard(store, randomstring):
    pld = randomstring().encode()
    key = randomstring()

    assert key not in store
    store.discard(key)
    assert key not in store
    store[key] = pld
    assert key in store

    store.discard(key)
    assert key not in store


def test_store_get(store, randomstring):
    pld = randomstring().encode()
    key = randomstring()

    retrieved = store.get(key)
    assert retrieved is None

    retrieved = store.get(key, pld)
    assert retrieved == pld

    pld = randomstring().encode()
    store[key] = pld
    retrieved = store.get(key)
    assert pld == retrieved


def test_store_pop(store, randomstring):
    pld = randomstring().encode()
    key = randomstring()
    store[key] = pld
    assert key in store

    retrieved = store.pop(key)
    assert pld == retrieved

    with pytest.raises(FileNotFoundError):
        store.pop(key)
    retrieved = store.pop(key, pld)
    assert retrieved == pld


def test_store_clear(store, randomstring):
    store.clear()  # should not raise

    num_keys = random.randint(1, 10)
    to_add = [f"key_{num}" for num in range(num_keys)]
    for k in to_add:
        store[k] = randomstring().encode()
    assert all(k in store for k in to_add)

    store.clear()
    assert not any(k in store for k in to_add)


def test_store_iteration(store, randomstring):
    num_keys = random.randint(1, 10)
    expected = {f"key_{num}": randomstring().encode() for num in range(num_keys)}
    for k, v in expected.items():
        store[k] = v

    for key, msg_bytes in store:
        assert expected[key] == msg_bytes


def test_store_repr(store):
    assert repr(store).startswith(store.__class__.__name__)
    assert "data_dir: " in repr(store)
    assert str(store.data_path) in repr(store)
