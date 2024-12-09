import concurrent.futures
import typing as t

import globus_compute_sdk as gc
import pytest
from globus_compute_sdk import Client, Executor
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


@pytest.mark.skipif(sdk_version.release < (2, 2, 5), reason="batch.add iface updated")
def test_batch(compute_client: Client, endpoint: str, linear_backoff: t.Callable):
    """Test batch submission and get_batch_result"""

    double_fn_id = compute_client.register_function(double)

    inputs = list(range(10))
    batch = compute_client.create_batch()

    for x in inputs:
        batch.add(double_fn_id, args=(x,))

    batch_res = compute_client.batch_run(endpoint, batch)
    tasks = [t for tl in batch_res["tasks"].values() for t in tl]

    while linear_backoff():
        results = compute_client.get_batch_result(tasks)
        try:
            total = sum(results[tid]["result"] for tid in results)
            break
        except KeyError:
            pass

    assert total == 2 * (sum(inputs)), "Batch run results do not add up"


def test_wait_on_new_hello_world_func(
    compute_client: Client, endpoint, linear_backoff: t.Callable
):
    func_id = compute_client.register_function(ohai)
    task_id = compute_client.run(endpoint_id=endpoint, function_id=func_id)

    while linear_backoff():
        try:
            result = compute_client.get_result(task_id)
            break
        except TaskPending:
            pass

    assert result == "ohai"


def test_executor(compute_client, endpoint, tutorial_function_id, timeout_s: int):
    """Test using Executor to retrieve results."""
    res = compute_client.web_client.get_version()
    assert res.http_status == 200, f"Received {res.http_status} instead!"

    num_tasks = 10
    submit_count = 2  # we've had at least one bug that prevented executor re-use

    # use client on newer versions and funcx_client on older
    try:
        gce = Executor(endpoint_id=endpoint, client=compute_client)
    except TypeError:
        gce = Executor(endpoint_id=endpoint, funcx_client=compute_client)

    with gce:
        for _ in range(submit_count):
            futures = [
                gce.submit_to_registered_function(tutorial_function_id)
                for _ in range(num_tasks)
            ]

            results = []
            for f in concurrent.futures.as_completed(futures, timeout=timeout_s):
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
        for f in concurrent.futures.as_completed(futures, timeout=timeout_s):
            results.append(f.result())
        assert all(
            "Hello World!" == item for item in results
        ), f"Invalid result: {results}"
