from funcx.sdk.client import FuncXClient
import time

fx = FuncXClient()


def test_batch1(a, b, c=2, d=2):
    return a + b + c + d


def test_batch2(a, b, c=2, d=2):
    return a * b * c * d


def test_batch3(a, b, c=2, d=2):
    return a + 2 * b + 3 * c + 4 * d


funcs = [test_batch1, test_batch2, test_batch3]
func_ids = []
for func in funcs:
    func_ids.append(fx.register_function(func, description='test'))

ep_id = '4b116d3c-1703-4f8f-9f6f-39921e5864df'
print("FN_UUID : ", func_ids)

start = time.time()
task_count = 5
batch = fx.create_batch()
for func_id in func_ids:
    for i in range(task_count):
        batch.add(i, i + 1, c=i + 2, d=i + 3, endpoint_id=ep_id, function_id=func_id)

task_ids = fx.batch_run(batch)

delta = time.time() - start
print("Time to launch {} tasks: {:8.3f} s".format(task_count * len(func_ids), delta))
print("Got {} tasks_ids ".format(len(task_ids)))

for i in range(10):
    x = fx.get_batch_status(task_ids)
    complete_count = sum([1 for t in task_ids if t in x and x[t].get('pending', False)])
    print("Batch status : {}/{} complete".format(complete_count, len(task_ids)))
    if complete_count == len(task_ids):
        print(x)
        break
    time.sleep(5)
