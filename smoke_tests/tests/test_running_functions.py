import concurrent.futures
import time

import globus_compute_sdk as gc
import pytest
import requests
from globus_compute_sdk import Executor
from packaging.version import Version

try:
    from globus_compute_sdk.errors import TaskPending
except ImportError:
    from globus_compute_sdk.utils.errors import TaskPending


sdk_version = Version(gc.version.__version__)


def test_run_pre_registered_function(
    endpoint, tutorial_function_id, submit_function_and_get_result
):
    """This test confirms that we are connected to the default production DB"""
    r = submit_function_and_get_result(endpoint, func=tutorial_function_id)
    assert r.result == "Hello World!"


def double(x):
    return x * 2


def ohai():
    import time

    time.sleep(5)
    return "ohai"


@pytest.mark.skipif(sdk_version.release < (1, 0, 5), reason="batch.add iface updated")
def test_batch(compute_client, endpoint):
    """Test batch submission and get_batch_result"""

    double_fn_id = compute_client.register_function(double)

    inputs = list(range(10))
    batch = compute_client.create_batch()

    for x in inputs:
        batch.add(double_fn_id, endpoint, args=(x,))

    batch_res = compute_client.batch_run(batch)

    total = 0
    for _i in range(12):
        time.sleep(5)
        results = compute_client.get_batch_result(batch_res)
        try:
            total = sum(results[tid]["result"] for tid in results)
            break
        except KeyError:
            pass

    assert total == 2 * (sum(inputs)), "Batch run results do not add up"


def test_wait_on_new_hello_world_func(compute_client, endpoint):
    func_id = compute_client.register_function(ohai)
    task_id = compute_client.run(endpoint_id=endpoint, function_id=func_id)

    got_result = False
    for _ in range(30):
        try:
            result = compute_client.get_result(task_id)
            got_result = True
        except TaskPending:
            time.sleep(1)

    assert got_result
    assert result == "ohai"


def test_executor(compute_client, endpoint, tutorial_function_id):
    """Test using Executor to retrieve results."""

    url = f"{compute_client.funcx_service_address}/version"
    res = requests.get(url)

    assert res.status_code == 200, f"Received {res.status_code} instead!"
    server_version = Version(res.json())
    if server_version.release < (1, 0, 5):
        pytest.skip(
            "Server too old (use `tox -- -v` for details)"
            "\n  Executor test requires the server to be at least v1.0.5."
            f"\n          Request: {url}"
            f"\n    Found version: v{server_version.public}"
        )

    num_tasks = 10
    submit_count = 2  # we've had at least one bug that prevented executor re-use

    with Executor(endpoint_id=endpoint, funcx_client=compute_client) as gce:
        for _ in range(submit_count):
            futures = [
                gce.submit_to_registered_function(tutorial_function_id)
                for _ in range(num_tasks)
            ]

            results = []
            for f in concurrent.futures.as_completed(futures, timeout=30):
                results.append(f.result())

            assert (
                len(results) == num_tasks
            ), f"Expected {num_tasks} results; received: {len(results)}"
            assert all(
                "Hello World!" == item for item in results
            ), f"Invalid result: {results}"

        futures = list(gce.reload_tasks())
        assert len(futures) == submit_count * num_tasks

        results = []
        for f in concurrent.futures.as_completed(futures, timeout=30):
            results.append(f.result())
        assert all(
            "Hello World!" == item for item in results
        ), f"Invalid result: {results}"
