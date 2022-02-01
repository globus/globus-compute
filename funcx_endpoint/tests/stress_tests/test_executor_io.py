import random

import pytest
from shared import simple_function


@pytest.mark.parametrize("task_count", [5000])
def test_bag_of_IO_tasks_executor(fx, endpoint, task_count):
    """Launches a parametrized bag of tasks using the executor interface."""
    data_sizes = [2000, 20000, 200000]
    futures = {}
    for _i in range(task_count):
        kwargs = {
            "input_data": bytearray(random.choice(data_sizes)),
            "output_size": random.choice(data_sizes),
        }
        future = fx.submit(simple_function, sleep_dur=0, **kwargs, endpoint_id=endpoint)
        futures[future] = kwargs

    print(futures)
    for future in futures:
        print("Trying future: ", future)
        x = future.result()
        expected_result = (
            len(futures[future]["input_data"]),
            bytearray(futures[future]["output_size"]),
        )
        assert x == expected_result, f"Result does not match, got: {x}"
