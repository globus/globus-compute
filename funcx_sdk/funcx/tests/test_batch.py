from funcx.sdk.client import FuncXClient
import pytest
import time


def fn_batch1(a, b, c=2, d=2):
    return a + b + c + d


def fn_batch2(a, b, c=2, d=2):
    return a * b * c * d


def fn_batch3(a, b, c=2, d=2):
    return a + 2 * b + 3 * c + 4 * d


def test_batch(fxc, endpoint):

    funcs = [fn_batch1, fn_batch2, fn_batch3]
    func_ids = []
    for func in funcs:
        func_ids.append(fxc.register_function(func, description='test'))

    start = time.time()
    task_count = 5
    batch = fxc.create_batch()
    for func_id in func_ids:
        for i in range(task_count):
            batch.add(i, i + 1, c=i + 2, d=i + 3, endpoint_id=endpoint, function_id=func_id)

    task_ids = fxc.batch_run(batch)

    delta = time.time() - start
    print("Time to launch {} tasks: {:8.3f} s".format(task_count * len(func_ids), delta))
    print("Got {} tasks_ids ".format(len(task_ids)))

    for i in range(10):
        x = fxc.get_batch_result(task_ids)
        complete_count = sum([1 for t in task_ids if t in x and not x[t].get('pending', False)])
        print("Batch status : {}/{} complete".format(complete_count, len(task_ids)))
        if complete_count == len(task_ids):
            print(x)
            break
        time.sleep(5)
