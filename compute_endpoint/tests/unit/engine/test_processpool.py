from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GCFuture, ProcessPoolEngine

_MOCK_BASE = "globus_compute_endpoint.engines.process_pool."


@pytest.fixture
def eng(endpoint_uuid):
    with mock.patch(f"{_MOCK_BASE}NativeExecutor"):
        ppe = ProcessPoolEngine()
        ppe.start(endpoint_id=endpoint_uuid)
        yield ppe
        ppe.shutdown(block=True)


def test_sets_task_id(eng, task_uuid):
    f = GCFuture(task_uuid)
    eng.submit(f, b"bytes", {})
    assert f.executor_task_id is not None


def test_rejects_resource_spec(eng, task_uuid):
    f = GCFuture(task_uuid)
    eng.submit(f, b"bytes", {"some": "resource_spec"})
    raw = f.result()
    res = messagepack.unpack(raw)

    assert "resource_specification is not supported" in res.data, "Expect eplanation"
    assert "For MPI apps, use GlobusMPIEngine" in res.data, "Expect suggestion"
    assert res.error_details is not None
