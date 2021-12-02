import pytest
from shared import simple_function


# @pytest.mark.parametrize("task_count", [100, 1000, 10000])
@pytest.mark.parametrize("task_count", [1000])
def test_IO_through_s3_executor(fx, endpoint, task_count, result_size=350000):
    """Launches N tasks that return ~500KB results per task."""
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

    for future in futures:
        print("Trying future: ", future)
        x = future.result()
        expected_result = (
            len(futures[future]["input_data"]),
            bytearray(futures[future]["output_size"]),
        )
        assert x == expected_result, f"Result does not match, got: {x}"
