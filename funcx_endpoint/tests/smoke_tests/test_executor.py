import random


def double(x):
    return x * 2


def test_executor_basic(fx, endpoint):
    """Test executor interface"""

    x = random.randint(0, 100)
    fut = fx.submit(double, x, endpoint_id=endpoint)

    assert fut.result(timeout=60) == x * 2, "Got wrong answer"
