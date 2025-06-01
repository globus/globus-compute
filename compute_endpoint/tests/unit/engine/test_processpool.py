from unittest import mock

from globus_compute_endpoint.engines import GCFuture, ProcessPoolEngine

_MOCK_BASE = "globus_compute_endpoint.engines.process_pool."


def test_sets_task_id(endpoint_uuid, task_uuid):
    with mock.patch(f"{_MOCK_BASE}NativeExecutor"):
        eng = ProcessPoolEngine()
        eng.start(endpoint_id=endpoint_uuid)
        f = GCFuture(task_uuid)
        eng.submit(f, b"bytes", {})
        assert f.executor_task_id is not None
        eng.shutdown()
