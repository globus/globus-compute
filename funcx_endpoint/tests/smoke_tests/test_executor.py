import random


def double(x):
    return x * 2


def test_executor_basic(fx, try_tutorial_endpoint):
    """Test executor interface"""

    x = random.randint(0, 100)
    fut = fx.submit(double, x, endpoint_id=try_tutorial_endpoint)

    assert fut.result(timeout=10) == x * 2, "Got wrong answer"
