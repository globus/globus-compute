import time

import pytest
from shared import simple_function


def wait_for_task(fxc, task_id, walltime: int = 2):

    start = time.time()
    while True:
        if time.time() > start + walltime:
            raise Exception("Timeout")
        try:
            r = fxc.get_result(task_id)
        except Exception:
            print("Not available yet")
            time.sleep(1)
        else:
            return r


@pytest.mark.parametrize("task_count", [10])
def test_bag_of_tasks_batch(fxc, endpoint, task_count, batch_size=10):
    """Launches a parametrized bag of tasks using the REST interface.
    Minimal data IO.
    """
    task_ids = []
    fn_uuid = fxc.register_function(simple_function, endpoint)

    for _ in range(task_count):
        batch = fxc.create_batch()
        for _ in range(batch_size):
            batch.add(
                input_data=bytearray(10),
                output_size=10,
                sleep_dur=0,
                function_id=fn_uuid,
                endpoint_id=endpoint,
            )
        batch_task_ids = fxc.batch_run(batch)
        task_ids.extend(batch_task_ids)

    for tid in task_ids:
        x = wait_for_task(fxc, tid, walltime=30)
        assert x == (10, bytearray(10)), f"Result does not match, got: {x}"
