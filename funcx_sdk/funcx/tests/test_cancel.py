import logging
import os
import random
import time
import uuid
from concurrent.futures import CancelledError

from parsl.providers import LocalProvider

import funcx
from funcx_endpoint.executors import HighThroughputExecutor

try:
    os.remove("interchange.log")
except Exception:
    pass

htex = HighThroughputExecutor(
    worker_debug=True,
    max_workers_per_node=1,
    passthrough=False,
    endpoint_id=str(uuid.uuid4()),
    provider=LocalProvider(
        init_blocks=1,
        min_blocks=1,
        max_blocks=1,
    ),
    run_dir=".",
)

htex.start()


def double(x):
    return x * 2


def slow_double(x, sleep_dur=2):
    import time

    time.sleep(sleep_dur)
    return x * 2


def test_non_cancel():
    n = 2
    future = htex.submit(double, n)
    print(future.task_id)
    assert future.result() == n * 2, "Got wrong answer"


def test_non_cancel_slow(t=2):
    future = htex.submit(slow_double, 5, sleep_dur=t)
    print(f"Future:{future}, status:{future.done()}")
    print(f"Task_id:{future.task_id}")
    print(f"Result:{future.result()}")


def test_cancel_slow(t=10):
    future = htex.submit(slow_double, 5, sleep_dur=t)
    print(f"Future: {future}, status:{future.done()}")
    print(f"Task_id: {future.task_id}")
    print("Sleeping")
    time.sleep(5)
    print("Cancelling")
    future.cancel()
    print(f"Cancelled, now status done={future.done()}")
    try:
        future.result()
    except CancelledError:
        print("Got the right error")


def test_cancel_task_pending_on_interchange():

    future1 = htex.submit(slow_double, 1, sleep_dur=5)
    future2 = htex.submit(slow_double, 2, sleep_dur=0)
    future2.cancel()
    future1.result()
    try:
        future2.result()  # This should raise a CancelledError
    except CancelledError:
        print("Got right error")
    else:
        raise Exception("Wrong exception or return value")


def test_cancel_random_tasks():

    futures = [htex.submit(slow_double, i, sleep_dur=2) for i in range(10)]
    random.shuffle(futures)
    [fu.cancel() for fu in futures[0:5]]
    for fu in futures[0:5]:
        try:
            fu.result()
        except CancelledError:
            print("Got the right error")
        else:
            raise Exception("Failed")
    for fu in futures[5:]:
        print(fu.result())


if __name__ == "__main__":

    test_non_cancel()
    test_non_cancel_slow(t=2)
    test_cancel_slow()
    test_cancel_task_pending_on_interchange()
    test_cancel_random_tasks()
    htex.shutdown()
