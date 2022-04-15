import random
import time


def double(x):
    return x * 2


def failing_task():
    raise IndexError()


def delay_n(n):
    import time

    time.sleep(n)
    return n


def noop():
    return


def test_random_delay(fx, endpoint, base_delay=600, n=10):
    """Tests tasks that run 10mins which is the websocket disconnect period"""

    futures = {}
    for _i in range(n):
        delay = base_delay + random.randint(10, 30)
        fut = fx.submit(delay_n, delay, endpoint_id=endpoint)
        futures[fut] = delay

    time.sleep(3)

    for fut in futures:
        assert fut.result(timeout=700) == futures[fut]
        print(f"I slept for {fut.result()} seconds")
