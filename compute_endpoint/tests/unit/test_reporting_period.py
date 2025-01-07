import random
from unittest import mock

import pytest
from globus_compute_endpoint.engines.base import ReportingThread
from tests.utils import try_assert


def test_default_period():
    """Confirm that the default reporting period is maintained"""
    assert ReportingThread(target=mock.Mock(), args=[]).reporting_period == 30.0


@pytest.mark.parametrize("reporting_period", (0.1, 5, 11, 30.0))
def test_reporting_period(reporting_period):
    counter = mock.Mock()
    test_call_count = random.randint(1, 100)
    rt = ReportingThread(target=counter, args=[], reporting_period=reporting_period)

    assert rt.reporting_period == reporting_period, "Verify initializer"

    with mock.patch.object(rt, "_shutdown_event") as mock_evt:
        mock_evt.wait.side_effect = [False] * (test_call_count - 1) + [True]
        rt.start()
    try_assert(lambda: not rt._thread.is_alive(), "Verify shutdown")

    assert rt.reporting_period == reporting_period, "Should not change"
    assert test_call_count == counter.call_count
    for a, _k in mock_evt.wait.call_args_list:
        assert a[0] == reporting_period, "Test kernel; waited correct time?"


def test_target_exception_saved(randomstring):
    exc_text = randomstring()
    exc = MemoryError(exc_text)
    target = mock.Mock()
    target.side_effect = exc

    rt = ReportingThread(target=target, args=[])
    assert not rt.status.done(), "Verify test setup"
    rt.run_in_loop(target)
    assert exc_text in str(rt.status.exception())

    rt = ReportingThread(target=target, args=[])
    assert not rt.status.done(), "Verify test setup"
    target.side_effect = (None, None, exc)  # invoke loop at least once before boom
    with mock.patch.object(rt, "_shutdown_event") as mock_evt:
        mock_evt.wait.return_value = False
        rt.run_in_loop(target)
    assert exc_text in str(rt.status.exception())


def test_stop_reporting_thread():
    rt = ReportingThread(target=mock.Mock(), args=[])
    assert not rt._thread.is_alive(), "Verify test setup"
    rt.start()
    try_assert(rt._thread.is_alive)
    rt.stop()
    try_assert(lambda: not rt._thread.is_alive())
    assert rt.status.result() is None, "Expect clean thread shutdown"
