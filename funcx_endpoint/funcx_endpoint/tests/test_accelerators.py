"""Test launching a executor where we pin workers to specific executors"""


def slow_get_cuda(x):
    import os
    from time import sleep

    sleep(2)
    return os.environ.get("CUDA_VISIBLE_DEVICES")


def test_assign_workers(htex):
    futures = [htex.submit(slow_get_cuda, i) for i in range(4)]
    devices = {f.result() for f in futures}
    assert devices == {"0", "1"}
