from globus_compute_endpoint.engines import GCFuture, GlobusMPIEngine

_MOCK_BASE = "globus_compute_endpoint.engines.globus_mpi."


def test_sets_task_id(fs, mock_mpiex, endpoint_uuid, task_uuid):
    eng = GlobusMPIEngine(executor=mock_mpiex)
    eng.start(endpoint_id=endpoint_uuid, run_dir="/")
    f = GCFuture(gc_task_id=task_uuid)
    eng.submit(f, b"bytes", {})
    assert f.executor_task_id is not None
