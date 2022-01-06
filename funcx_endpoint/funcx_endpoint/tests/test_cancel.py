import logging
import os
import random
import time
import uuid
from concurrent.futures import CancelledError

import pytest
from parsl.providers import LocalProvider

from funcx_endpoint.executors import HighThroughputExecutor

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def htex():
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
    yield htex
    htex.shutdown()


def double(x):
    return x * 2


def slow_double(x, sleep_dur=2):
    import time

    time.sleep(sleep_dur)
    return x * 2


def test_cancel_notimplemented(htex):
    n = 2
    future = htex.submit(slow_double, n)
    logger.warning(f"Launched task:{future.task_id}")
    with pytest.raises(NotImplementedError):
        future.cancel()
    logger.warning(f"Finished task:{future.task_id}")


def test_non_cancel(htex):
    n = 2
    future = htex.submit(double, n)
    print(future.task_id)
    assert future.result() == n * 2, "Got wrong answer"


def test_non_cancel_slow(htex, t=1):
    future = htex.submit(slow_double, 5, sleep_dur=t)
    assert future.result() == 10


def test_cancel_slow(htex, t=10):
    future = htex.submit(slow_double, 5, sleep_dur=t)
    logger.warning(f"Launched task:{future.task_id}")
    time.sleep(5)
    logger.warning(f"Cancelling task:{future.task_id}")
    future.best_effort_cancel()
    logger.warning(f"Cancelled, now task:{future.task_id} is done:{future.done()}")
    with pytest.raises(CancelledError):
        future.result()


def test_cancel_task_pending_on_interchange(htex):

    future1 = htex.submit(slow_double, 1, sleep_dur=5)
    future2 = htex.submit(slow_double, 2, sleep_dur=0)
    future2.best_effort_cancel()
    future1.result()
    try:
        future2.result()  # This should raise a CancelledError
    except CancelledError:
        print("Got right error")
    else:
        raise Exception("Wrong exception or return value")


def test_cancel_random_tasks(htex):

    futures = [htex.submit(slow_double, i, sleep_dur=2) for i in range(10)]
    logger.warning(f"Launched 10 tasks {[f.task_id for f in futures]}")
    random.shuffle(futures)
    for fu in futures[0:5]:
        fu.best_effort_cancel()
        logger.warning(f"Cancelled task:{fu.task_id}")
    for fu in futures[0:5]:
        try:
            logger.warning(f"Waiting for cancelled task:{fu.task_id}")
            fu.result()
        except CancelledError:
            print("Got the right error")
        else:
            raise Exception("Failed")
    for fu in futures[5:]:
        logger.warning(f"Waiting for non-cancelled task:{fu.task_id}")
        print(fu.result())


def make_file_slow(fname, sleep_dur=2):
    import time

    time.sleep(sleep_dur)
    with open(fname, "w") as f:
        f.write("Hello")
    return fname


def test_cancel_random_file_creators(htex):

    fmap = {}
    for i in range(10):
        fname = f"{os.getcwd()}/hello.{i}.out"
        if os.path.exists(fname):
            os.remove(fname)
        future = htex.submit(make_file_slow, fname, sleep_dur=2)
        logger.warning(f"Launched task {future.task_id}")
        fmap[future] = {"fname": fname, "cancelled": False}
    print(fmap)

    keys = list(fmap.keys())
    random.shuffle(keys)
    to_cancel = keys[0:5]

    for future in to_cancel:
        future.best_effort_cancel()
        logger.warning(f"Cancelled task:{future.task_id}")
        fmap[future]["cancelled"] = True

    print("Here")
    for future in fmap:

        print("Checking:", fmap[future])
        if fmap[future]["cancelled"] is False:
            logger.warning(f"Waiting for non-cancelled task:{future.task_id}")
            assert fmap[future]["fname"] == future.result(), "Got wrong fname"
            assert os.path.exists(fmap[future]["fname"]), "Expected file is missing"
        else:
            try:
                logger.warning(f"Waiting for cancelled task:{future.task_id}")
                f = future.result()
            except CancelledError:
                print("Got the right error")
            else:
                raise Exception(f"Failed, got wrong exception: {f}")
            assert (
                os.path.exists(fmap[future]["fname"]) is False
            ), "Expected file is missing"
