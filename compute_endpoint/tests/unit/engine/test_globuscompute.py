import platform
import random
import uuid
from unittest import mock

import parsl
import pytest
from globus_compute_endpoint.engines import GCFuture, GlobusComputeEngine

_MOCK_BASE = "globus_compute_endpoint.engines.globus_compute."
this_py_version = platform.python_version()


@pytest.fixture(autouse=True)
def mock_jsp():
    with mock.patch(f"{_MOCK_BASE}JobStatusPoller", spec=True) as m:
        yield m


@pytest.fixture
def mock_blocks():
    return [str(i) for i in range(random.randint(1, 20))]


@pytest.fixture
def mock_managers(mock_blocks, randomstring):
    return [
        {
            "manager": randomstring(),
            "block_id": bid,
            "worker_count": random.randint(1, 32),
            "tasks": random.randint(0, 100),
            "idle_duration": max(0.0, random.random() * 100.0 - 10.0),
            "active": random.random() < 0.5,
            "parsl_version": parsl.__version__,
            "python_version": this_py_version,
            "draining": False,
        }
        for bid in mock_blocks
    ]


@pytest.fixture
def mock_managers_packages(mock_managers):
    return {
        m["manager"]: {
            "globus-compute-endpoint": "3.1.1",
            "globus-compute-sdk": "3.1.1",
        }
        for m in mock_managers
    }


@pytest.fixture
def htex_factory(mock_blocks, mock_managers, mock_managers_packages):
    def _htex_factory(**kwargs):
        kwargs.setdefault("address", "::1")
        _htex = parsl.HighThroughputExecutor(**kwargs)
        _htex.start = mock.Mock(spec=_htex.start)

        _htex.blocks_to_job_id.update((bid, str(int(bid) + 100)) for bid in mock_blocks)
        _htex.job_ids_to_block.update(
            (jid, bid) for bid, jid in _htex.blocks_to_job_id.items()
        )
        _htex.connected_managers = mock.Mock(spec=_htex.connected_managers)
        _htex.connected_managers.return_value = mock_managers
        _htex.connected_managers_packages = mock.Mock(
            spec=_htex.connected_managers_packages
        )
        _htex.connected_managers_packages.return_value = mock_managers_packages

        return _htex

    yield _htex_factory


@pytest.fixture
def htex(htex_factory):
    _htex = htex_factory()
    yield _htex
    _htex.shutdown()


def test_status_report_content(htex, mock_managers, mock_managers_packages):
    ep_id = uuid.uuid4()
    gce = GlobusComputeEngine(endpoint_id=ep_id, executor=htex)
    sr = gce.get_status_report()
    num_managers = len(mock_managers)
    num_active_managers = sum(m["active"] for m in mock_managers)
    num_live_workers = sum(m["worker_count"] for m in mock_managers)
    num_idle_workers = sum(
        max(0, m["worker_count"] - m["tasks"]) for m in mock_managers
    )
    assert sr.endpoint_id == ep_id
    assert num_managers == sr.global_state["managers"]
    assert num_active_managers == sr.global_state["active_managers"]
    assert num_live_workers == sr.global_state["total_workers"]
    assert num_idle_workers == sr.global_state["idle_workers"]
    assert type(gce).__name__ == sr.global_state["engine_type"]
    assert type(gce.executor.provider).__name__ == sr.global_state["provider_type"]

    assert "pending_tasks" in sr.global_state
    assert "outstanding_tasks" in sr.global_state
    assert "total_tasks" in sr.global_state
    assert "scaling_enabled" in sr.global_state
    assert "mem_per_worker" in sr.global_state
    assert "cores_per_worker" in sr.global_state
    assert "prefetch_capacity" in sr.global_state
    assert "max_blocks" in sr.global_state
    assert "min_blocks" in sr.global_state
    assert "max_workers_per_node" in sr.global_state
    assert "nodes_per_block" in sr.global_state
    assert "queue" in sr.global_state
    assert "account" in sr.global_state

    assert len(sr.global_state["node_info"]) == num_managers

    for mock_m in mock_managers:
        jid = htex.blocks_to_job_id[mock_m["block_id"]]
        packages = mock_managers_packages[mock_m["manager"]]
        m = sr.global_state["node_info"][jid][0]  # for now, test assumes one worker
        assert m["parsl_version"] == mock_m["parsl_version"]
        assert m["endpoint_version"] == packages["globus-compute-endpoint"]
        assert m["sdk_version"] == packages["globus-compute-sdk"]
        assert m["python_version"] == mock_m["python_version"]
        assert m["idle_duration"] == mock_m["idle_duration"]
        assert m["worker_count"] == mock_m["worker_count"]


@pytest.mark.parametrize("queue_term", ("queue", "partition"))
@pytest.mark.parametrize("account_term", ("account", "allocation", "project"))
def test_status_report_provider_specific_terms(
    htex_factory, queue_term: str, account_term: str, randomstring
):
    ep_id = uuid.uuid4()
    queue_val = randomstring()
    account_val = randomstring()

    class TestProvider(parsl.providers.LocalProvider):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            setattr(self, queue_term, queue_val)
            setattr(self, account_term, account_val)

    provider = TestProvider()
    htex = htex_factory(provider=provider)
    gce = GlobusComputeEngine(endpoint_id=ep_id, executor=htex)
    sr = gce.get_status_report()

    assert sr.global_state["queue"] == queue_val
    assert sr.global_state["account"] == account_val


def test_status_poller_started_late(tmp_path, htex, mock_jsp, ep_uuid):
    gce = GlobusComputeEngine(endpoint_id=ep_uuid, executor=htex)
    assert gce.job_status_poller is None, "JSP must be started *after* daemonization"
    assert not mock_jsp.called, "`.start()` not yet invoked."
    gce.start(endpoint_id=ep_uuid, run_dir=str(tmp_path))
    assert mock_jsp.called, "Expect JSP created in engine's process"
    assert gce.job_status_poller is not None
    a, _ = gce.job_status_poller.add_executors.call_args
    assert a[0] == [htex], "Expect executor to be polled"


def test_sets_task_id(tmp_path, mock_htex, endpoint_uuid, task_uuid):
    eng = GlobusComputeEngine(executor=mock_htex)
    eng.start(endpoint_id=endpoint_uuid, run_dir=str(tmp_path))
    f = GCFuture(task_uuid)
    eng.submit(f, b"bytes", {})
    assert f.executor_task_id is not None
    eng.shutdown()
