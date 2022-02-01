import pytest

from shared import simple_function


@pytest.mark.parametrize("task_count", [100, 1000])
def test_bag_of_tasks_executor(fx, endpoint, task_count):
    """Launches a parametrized bag of tasks using the executor interface.
    Minimal data IO.
    """
    futures = []
    for _i in range(task_count):
        future = fx.submit(
            simple_function,
            input_data=bytearray(10),
            output_size=10,
            sleep_dur=0,
            endpoint_id=endpoint,
        )
        futures.append(future)

    for future in futures:
        x = future.result()
        assert x == (10, bytearray(10)), f"Result does not match, got: {x}"
