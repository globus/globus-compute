import time

import pytest


def double(x):
    return x * 2


@pytest.mark.parametrize("task_count", [(10), (100), (1000)])
def test_performance(fxc, endpoint, task_count):

    func_id = fxc.register_function(double, description="double")

    start = time.time()
    batch = fxc.create_batch()

    for i in range(task_count):
        batch.add(i, endpoint_id=endpoint, function_id=func_id)

    t_batch_create = time.time() - start
    task_ids = fxc.batch_run(batch)
    t_batch_submit = time.time() - t_batch_create

    print(f"Time to create batch: {t_batch_create}s")
    print(f"Time to submit batch: {t_batch_submit}s")
    for _i in range(10):
        x = fxc.get_batch_result(task_ids)
        complete_count = sum(
            [1 for t in task_ids if t in x and not x[t].get("pending", False)]
        )
        print(f"Batch status : {complete_count}/{len(task_ids)} complete")
        if complete_count == len(task_ids):
            print(x)
            break
        time.sleep(2)
    t_finish = time.time() - start
    print(f"Time to launch {task_count} tasks: {t_finish:8.3f} s")
    print(f"Got {len(task_ids)} tasks_ids ")
