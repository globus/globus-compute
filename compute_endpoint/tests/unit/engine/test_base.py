import functools
import random
from concurrent.futures import Future
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GCFuture, ThreadPoolEngine
from globus_compute_endpoint.engines.base import (
    _EXC_HISTORY_TMPL,
    GlobusComputeEngineBase,
)
from globus_compute_endpoint.engines.helper import execute_task
from globus_compute_sdk.serialize import ComputeSerializer
from globus_compute_sdk.serialize.concretes import (
    SELECTABLE_CODE_STRATEGIES,
    SELECTABLE_DATA_STRATEGIES,
)
from tests.utils import double


class _Dict(dict):
    def __init__(self, *a, **k):
        self.deleted_items = []
        super().__init__(*a, **k)

    def pop(self, key, *a, **k):
        val = dict.pop(self, key, *a, **k)
        self.deleted_items.append((key, val))
        return val


@pytest.fixture()
def task_bytes(ez_pack_task):
    return ez_pack_task(double, 1)


@pytest.fixture
def eng(endpoint_uuid):
    tpe_mock_path = "globus_compute_endpoint.engines.thread_pool."
    with mock.patch(f"{tpe_mock_path}NativeExecutor"):
        tpe = ThreadPoolEngine()
        assert isinstance(tpe.executor, mock.Mock)
        tpe.start(endpoint_id=endpoint_uuid)
        try:
            yield tpe
        finally:
            tpe.shutdown(block=True)


@pytest.fixture
def eng_args(eng):
    a, k = [], {}

    def mock_invoke(_task_f, f_partial: functools.partial, *_a, **_k):
        a.extend(f_partial.args)
        k.update(f_partial.keywords)

    with mock.patch.object(eng, "_invoke_submission", mock_invoke):
        yield eng, a, k


def test_tasks_cleared_when_complete(eng, task_bytes, task_uuid):
    # important for memory that we clean up each task
    eng._task_id_map = _Dict()

    work_f = Future()
    work_f.set_result(None)
    work_f.executor_task_id = 123
    f = GCFuture(task_uuid)
    with mock.patch.object(eng, "_submit") as m:
        m.return_value = work_f
        eng.submit(f, task_bytes, {})

    f.result()  # ensure task is done before test continues
    assert len(eng._task_id_map) == 0
    ex_tid, ex_fut = eng._task_id_map.deleted_items[-1]
    assert ex_tid == f.executor_task_id
    assert ex_fut is f


@pytest.mark.parametrize("retries", range(5))
def test_invoke_honors_retry(eng, task_uuid, retries):
    eng.max_retries_on_system_failure = retries

    def create_work_f(*a, **k):
        work_f = Future()
        work_f.executor_task_id = 123
        work_f.set_exception(MemoryError())
        return work_f

    f = GCFuture(task_uuid)
    with mock.patch.object(eng, "_submit") as m:
        m.side_effect = create_work_f
        eng.submit(f, task_bytes, {})

    res = messagepack.unpack(f.result())
    assert MemoryError.__name__ in res.data

    sentinel_line = _EXC_HISTORY_TMPL.split("\n")[0]
    assert res.data.count(sentinel_line) == retries + 1
    assert m.call_count == retries + 1
    if retries:
        assert "final attempt" in res.data
        assert all(f"from attempt: {i}" in res.data for i in range(retries))
    else:
        assert "final attempt" not in res.data
        assert "from attempt: " not in res.data


def test_submit_uses_helper_execute(
    randomstring, eng_args, task_bytes, endpoint_uuid, task_uuid
):
    eng, a, k = eng_args
    f = GCFuture(task_uuid)

    eng.submit(f, task_bytes, {"a": randomstring()})

    assert execute_task in a, "Time to update this assumption?"
    assert task_bytes in a


def test_submit_sets_task_state(eng_args, task_bytes, endpoint_uuid, task_uuid):
    eng, a, k = eng_args
    f = GCFuture(task_uuid)

    eng.submit(f, task_bytes, {})

    args, kwargs = k["args"], k["kwargs"]
    assert task_uuid in args, "top-level (user-visible) task id should be shared"
    assert endpoint_uuid in args
    assert kwargs["run_dir"] == eng.working_dir
    assert kwargs["run_in_sandbox"] == eng.run_in_sandbox


def test_submit_specifies_serializers(randomstring, eng_args, task_bytes, task_uuid):
    eng, _a, k = eng_args
    f = GCFuture(task_uuid)

    res_serde = list(randomstring() for _ in range(random.randint(0, 10)))
    eng.submit(f, task_bytes, {}, result_serializers=res_serde)

    assert k["kwargs"]["result_serializers"] == res_serde


def test_submit_specifies_deserializers(eng_args, task_bytes, endpoint_uuid, task_uuid):
    num_desers_code = random.randint(1, len(SELECTABLE_CODE_STRATEGIES))
    num_desers_data = random.randint(1, len(SELECTABLE_DATA_STRATEGIES))
    desers = random.choices(SELECTABLE_CODE_STRATEGIES, k=num_desers_code)
    desers.extend(random.choices(SELECTABLE_DATA_STRATEGIES, k=num_desers_data))
    exp_task_deser = {f"{c.__module__}.{c.__name__}" for c in desers}

    eng, a, k = eng_args
    eng.serde = ComputeSerializer(allowed_deserializer_types=desers)

    f = GCFuture(task_uuid)

    eng.submit(f, task_bytes, {})

    assert exp_task_deser == set(
        k["kwargs"]["task_deserializers"]
    ), eng.serde.allowed_deserializer_types


def test_expected_abstract_methods(eng_args):
    exp_abstract_methods = {
        "assert_ha_compliant": (),
        "start": (),
        "get_status_report": (),
        "_submit": ({}, lambda: 1),
        "shutdown": (),
    }

    found_abstract_methods = {
        attr
        for attr in dir(GlobusComputeEngineBase)
        if "__isabstractmethod__" in dir(getattr(GlobusComputeEngineBase, attr))
    }

    def create_method(sup_cls, meth_name, *args):
        def _test_meth(self):
            getattr(sup_cls, meth_name)(self, *args)

        return _test_meth

    class _MockGCEng(GlobusComputeEngineBase):
        def __new__(cls, *args, **kwargs):
            sup_cls = super()
            for am_name, a in exp_abstract_methods.items():
                setattr(cls, am_name, create_method(sup_cls, am_name, *a))
            return cls

    assert found_abstract_methods == set(exp_abstract_methods), "Update test?"

    test_eng = _MockGCEng()
    for am_name in exp_abstract_methods:
        with pytest.raises(NotImplementedError):
            getattr(test_eng, am_name)(test_eng)
