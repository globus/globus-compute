import random
import sys
import uuid
from unittest import mock

import parsl
import pytest
from globus_compute_endpoint.engines import GlobusComputeEngine

_MOCK_BASE = "globus_compute_endpoint.engines.globus_compute."
this_py_version = "{}.{}.{}".format(*sys.version_info)


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
def htex(mock_blocks, mock_managers):
    _htex = parsl.HighThroughputExecutor(address="::1")

    _htex.blocks_to_job_id.update((bid, str(int(bid) + 100)) for bid in mock_blocks)
    _htex.job_ids_to_block.update(
        (jid, bid) for bid, jid in _htex.blocks_to_job_id.items()
    )
    _htex.connected_managers = mock.Mock(spec=_htex.connected_managers)
    _htex.connected_managers.return_value = mock_managers

    yield _htex


def test_status_report_content(htex, mock_managers):
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

    assert "pending_tasks" in sr.global_state
    assert "outstanding_tasks" in sr.global_state
    assert "scaling_enabled" in sr.global_state
    assert "mem_per_worker" in sr.global_state
    assert "cores_per_worker" in sr.global_state
    assert "prefetch_capacity" in sr.global_state
    assert "max_blocks" in sr.global_state
    assert "min_blocks" in sr.global_state
    assert "max_workers_per_node" in sr.global_state
    assert "nodes_per_block" in sr.global_state

    assert len(sr.global_state["node_info"]) == num_managers

    for mock_m in mock_managers:
        jid = htex.blocks_to_job_id[mock_m["block_id"]]
        m = sr.global_state["node_info"][jid][0]  # for now, test assumes one worker
        assert m["parsl_version"] == mock_m["parsl_version"]
        assert m["python_version"] == mock_m["python_version"]
        assert m["idle_duration"] == mock_m["idle_duration"]
        assert m["worker_count"] == mock_m["worker_count"]
