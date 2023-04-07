import random

from globus_compute_sdk.sdk.executor import AtomicController


class DummyTarget:
    def __init__(self):
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


def test_testtool_verification():
    x = DummyTarget()
    assert not x.started, "Test setup verification"
    assert not x.stopped, "Test setup verification"


def test_atomic_increment_default():
    x = DummyTarget()

    ac = AtomicController(x.start, x.stop)
    assert ac.value() == 0
    assert not x.started
    assert not x.stopped

    ac.increment()
    assert ac.value() == 1
    assert x.started
    assert not x.stopped

    x.started = False

    ac.increment()
    assert ac.value() == 2
    assert not x.started
    assert not x.stopped

    ac.decrement()
    assert ac.value() == 1
    assert not x.started
    assert not x.stopped

    ac.decrement()
    assert ac.value() == 0
    assert not x.started
    assert x.stopped


def test_atomic_increment_by_n():
    x = DummyTarget()
    ac = AtomicController(x.start, x.stop)

    num_tasks = random.randint(1, 20)
    ac.increment(num_tasks)
    assert ac.value() == num_tasks
    assert x.started
    assert not x.stopped

    x.started = False
    while num_tasks > 1:
        ac.decrement()
        assert not x.started
        assert not x.stopped
        num_tasks -= 1

    ac.decrement()
    assert ac.value() == 0
    assert not x.started
    assert x.stopped


def test_atomic_reset():
    x = DummyTarget()
    ac = AtomicController(x.start, x.stop)

    some_value = random.randint(1, 20)
    ac.increment(some_value)
    x.started = False

    assert ac.value() == some_value
    ac.reset()
    assert not x.started
    assert not x.stopped
    assert ac.value() == 0
