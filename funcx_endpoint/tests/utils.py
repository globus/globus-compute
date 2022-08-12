import time
import typing as t


def try_for_timeout(
    test_func: t.Callable, timeout_ms: int = 5000, check_period_ms: int = 20
) -> bool:
    timeout_s = abs(timeout_ms) / 1000.0
    check_period_s = abs(check_period_ms) / 1000.0
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        if test_func():
            return True
        time.sleep(check_period_s)
    return False
