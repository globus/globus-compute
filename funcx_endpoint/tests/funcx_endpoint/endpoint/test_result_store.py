import os.path
import pickle
import queue
import tempfile

import pytest

from funcx_endpoint.endpoint.results_store import ResultsStore


@pytest.fixture()
def tmp_store_path():
    store_path = tempfile.TemporaryDirectory()
    yield store_path.name
    store_path.cleanup()


def test_simple_store_and_get(tmp_store_path):

    store = ResultsStore(tmp_store_path)
    message = "hello"
    result_path = store.put("001", message)
    assert os.path.exists(result_path)
    with open(result_path, "rb") as f:
        data = pickle.load(f)
        assert data == message

    task_id, fetched = store.get()
    assert fetched == message
    store.pop(task_id)


def test_empty_store(tmp_store_path):

    store = ResultsStore(tmp_store_path)

    with pytest.raises(queue.Empty):
        store.get()

    store.put("001", "hello")
    task_id, message = store.get()
    store.pop(task_id)

    with pytest.raises(queue.Empty):
        store.get()


def test_nonexistent_task_pop(tmp_store_path):
    store = ResultsStore(tmp_store_path)

    store.put("001", "hello")
    task_id, message = store.get()
    store.pop(task_id)

    with pytest.raises(FileNotFoundError):
        store.pop("002")


def test_iteration(tmp_store_path):

    store = ResultsStore(tmp_store_path)
    store.put("001", 1)
    store.put("002", 2)
    original = {1, 2}

    target = set()
    while True:
        try:
            tid, val = store.get()
            store.pop(tid)
            target.add(val)
        except queue.Empty:
            break

    assert target == original
