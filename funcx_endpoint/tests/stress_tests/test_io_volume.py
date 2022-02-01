import logging
import time

import pytest
from shared import simple_function

logger = logging.getLogger("test")


@pytest.mark.parametrize("task_count", [10000])
def test_small_IO_through_s3_executor(fx, endpoint, task_count, result_size=100000):
    """Launches N tasks that return ~1MB results per task."""
    futures = {}
    for _i in range(task_count):
        kwargs = {"input_data": bytearray(1), "output_size": result_size}
        future = fx.submit(simple_function, sleep_dur=0, **kwargs, endpoint_id=endpoint)
        futures[future] = kwargs

    for future in futures:
        print("Trying future: ", future)
        x = future.result()
        expected_result = (
            len(futures[future]["input_data"]),
            bytearray(futures[future]["output_size"]),
        )
        assert x == expected_result, f"Result does not match, got: {x}"


@pytest.mark.parametrize("task_count", [10000])
def test_IO_through_redis_executor(fx, endpoint, task_count, result_size=10000):
    """Launches N tasks that return ~10KB results per task that would use redis"""
    futures = {}
    for _i in range(task_count):
        kwargs = {"input_data": bytearray(1), "output_size": result_size}
        future = fx.submit(simple_function, sleep_dur=0, **kwargs, endpoint_id=endpoint)
        futures[future] = kwargs
        # logger.warning(f"Launching futures with {future.task_id}")

    for future in futures:
        print("Trying future: ", future)
        try:
            x = future.result(timeout=600)
        except TimeoutError:
            logger.warning(f"Failed to get results for task_id:{future.task_id}")
            raise
        expected_result = (
            len(futures[future]["input_data"]),
            bytearray(futures[future]["output_size"]),
        )
        assert x == expected_result, f"Result does not match, got: {x}"


@pytest.mark.parametrize("task_count", [10])
def test_IO_through_S3_executor(fx, endpoint, task_count, result_size=7000000):
    """Launches N tasks that return ~7MB results per task that would use redis"""
    futures = {}
    count = 0
    for _i in range(task_count):
        kwargs = {"input_data": bytearray(result_size), "output_size": result_size}
        future = fx.submit(simple_function, sleep_dur=0, **kwargs, endpoint_id=endpoint)
        futures[future] = kwargs
        count += 1
        if count % 1000 is True:
            logger.warning(f"Task {count}/{task_count} launched")
        # logger.warning(f"Launching futures with {future.task_id}")

    count = 0
    for future in futures:
        print("Trying future: ", future)
        try:
            x = future.result(timeout=600)
            count += 1
        except TimeoutError:
            logger.warning(f"Failed to get results for task_id:{future.task_id}")
            raise
        logger.warning(f"Task {count}/{task_count} completed")
        expected_result = (
            result_size,
            bytearray(result_size),
        )
        assert x == expected_result, f"Result does not match, got: {x}"


@pytest.mark.parametrize("io_size", [1, 10, 100, 1000, 10000, 100000, 1000000])
def test_IO_latency(fx, endpoint, io_size, task_count=10):
    """Launches N tasks that return ~1MB results per task."""

    s_times = []
    ttc_times = []
    for _i in range(task_count):
        kwargs = {"input_data": bytearray(io_size), "output_size": io_size}
        s = time.time()
        future = fx.submit(simple_function, sleep_dur=0, **kwargs, endpoint_id=endpoint)
        submit = time.time() - s
        future.result(timeout=60)
        done = time.time() - s
        s_times.append(submit)
        ttc_times.append(done)

    avg_submit_time = sum(s_times) / len(s_times)
    avg_ttc_time = sum(ttc_times) / len(ttc_times)
    logger.warning(
        f"Submit latency over {task_count} iterations with IO of {io_size}:"
        f" min={min(s_times):.3f}s max={max(s_times):.3f}s avg:{avg_submit_time:.3f}s"
    )
    logger.warning(
        f"TTC over {task_count} iterations with IO of {io_size}: "
        f"min={min(ttc_times):.3f}s max={max(ttc_times):.3f}s avg:{avg_ttc_time:.3f}s"
    )
