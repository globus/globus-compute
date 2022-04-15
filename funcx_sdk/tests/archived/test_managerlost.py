import pytest

from funcx_endpoint.executors.high_throughput.interchange import ManagerLost


def kill_manager_sometimes(kill=True):
    import os
    import signal

    import psutil

    parent = os.getppid()
    if kill is True:
        return os.kill(parent, signal.SIGTERM)
    else:
        manager_proc = psutil.Process(parent)
        return str(manager_proc.cmdline()), manager_proc.name()


def test_manager(fx, endpoint):

    future = fx.submit(kill_manager_sometimes, kill=False, endpoint_id=endpoint)

    assert future.result(timeout=200) != 0
    pid, name = future.result()
    assert "funcx-manager" in name


def test_manager_lost(fx, endpoint):

    future = fx.submit(kill_manager_sometimes, kill=True, endpoint_id=endpoint)

    with pytest.raises(ManagerLost):
        assert future.result(timeout=200) != 0


def test_endpoint_not_borked(fx, endpoint):

    futs = []
    for _i in range(10):
        future = fx.submit(kill_manager_sometimes, kill=False, endpoint_id=endpoint)
        futs.append(future)

    for fu in futs:
        assert fu.result(timeout=200) != 0
