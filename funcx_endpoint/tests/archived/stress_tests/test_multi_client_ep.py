import logging
import os
from functools import partial
from multiprocessing import Pool

import pytest
from shared import simple_function

from funcx import FuncXClient
from funcx.sdk.executor import FuncXExecutor

logger = logging.getLogger("test")


def double(x):
    return x * 2


def run_tasks_with_fresh_client(
    funcx_test_config, endpoint, task_count=10, io_size=100
):
    """Create a fresh executor and run tests that will hit only redis"""
    pid = os.getpid()
    logger.warning(f"Creating client on pid:{pid} against endpoint:{endpoint}")

    client_args = funcx_test_config["client_args"]
    fxc = FuncXClient(**client_args)
    fx = FuncXExecutor(fxc)

    futures = {}
    for t in range(task_count):
        futures[t] = fx.submit(
            simple_function,
            sleep_dur=0,
            input_data=bytearray(io_size),
            output_size=io_size,
            endpoint_id=endpoint,
        )

    for t in futures:
        x = futures[t].result(timeout=300)
        expected_result = (io_size, bytearray(io_size))
        assert x == expected_result
    return


@pytest.mark.parametrize("task_count", [1000])
def test_multi_client_small_data(funcx_test_config, endpoints, task_count, io_size=100):

    fn = partial(
        run_tasks_with_fresh_client, funcx_test_config, task_count=10, io_size=100
    )
    with Pool(len(endpoints)) as p:
        p.map(fn, endpoints)


@pytest.mark.parametrize("task_count", [100])
def test_multi_client_large_data(
    funcx_test_config, endpoints, task_count, io_size=100000
):

    fn = partial(
        run_tasks_with_fresh_client, funcx_test_config, task_count=10, io_size=100
    )
    with Pool(len(endpoints)) as p:
        p.map(fn, endpoints)
