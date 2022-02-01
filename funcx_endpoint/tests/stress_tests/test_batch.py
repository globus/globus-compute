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


@pytest.mark.parametrize("task_count", [100])
@pytest.mark.skip
def test_bag_of_tasks_batch(fxc, endpoint, task_count, batch_size=100):
    """Launches a parametrized bag of tasks using the REST interface.
    Minimal data IO.
    """
    task_ids = []
    fn_uuid = fxc.register_function(simple_function, endpoint)

    batch = None
    batch_ids = []
    for i in range(task_count):
        if i % 100:
            # if a batch exists launch, and then create new batch
            if batch:
                b_id = fxc.batch_run(batch)
                batch_ids.append(b_id)
            batch = fxc.create_batch()

    batch = fxc.create_batch()

    for _i in range(task_count):
        tid = fxc.run(
            input_data=bytearray(10),
            output_size=10,
            sleep_dur=0,
            function_id=fn_uuid,
            endpoint_id=endpoint,
        )
        task_ids.append(tid)

    for tid in task_ids:
        x = wait_for_task(fxc, tid, walltime=30)
        assert x == (10, bytearray(10)), f"Result does not match, got: {x}"
