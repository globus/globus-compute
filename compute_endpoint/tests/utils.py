import itertools
import pathlib
import sys
import time
import types
import typing as t


def create_traceback(start: int = 0) -> types.TracebackType:
    """
    Dynamically create a traceback.

    Builds a traceback from the top of the stack (the currently executing frame) on
    down to the root frame.  Optionally, use start to build from an earlier stack
    frame.
    """
    tb = None
    for depth in itertools.count(start + 1, 1):
        try:
            frame = sys._getframe(depth)
            tb = types.TracebackType(tb, frame, frame.f_lasti, frame.f_lineno)
        except ValueError:
            break
    return tb


def try_assert(
    test_func: t.Callable[[], bool],
    fail_msg: str = "",
    timeout_ms: float = 5000,
    attempts: int = 0,
    check_period_ms: int = 20,
):
    tb = create_traceback(start=1)
    timeout_s = abs(timeout_ms) / 1000.0
    check_period_s = abs(check_period_ms) / 1000.0
    if attempts > 0:
        for _attempt_no in range(attempts):
            if test_func():
                return
            time.sleep(check_period_s)
        else:
            att_fail = (
                f"\n  (Still failing after attempt limit [{attempts}], testing every"
                f" {check_period_ms}ms)"
            )
            raise AssertionError(f"{str(fail_msg)}{att_fail}".strip()).with_traceback(
                tb
            )

    elif timeout_s > 0:
        end = time.monotonic() + timeout_s
        while time.monotonic() < end:
            if test_func():
                return
            time.sleep(check_period_s)
        att_fail = (
            f"\n  (Still failing after timeout [{timeout_ms}ms], with attempts "
            f"every {check_period_ms}ms)"
        )
        raise AssertionError(f"{str(fail_msg)}{att_fail}".strip()).with_traceback(tb)

    else:
        raise AssertionError("Bad test configuration: no attempts or timeout period")


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


def ez_pack_function(serializer, func, args, kwargs):
    serialized_func = serializer.serialize(func)
    serialized_args = serializer.serialize(args)
    serialized_kwargs = serializer.serialize(kwargs)
    return serializer.pack_buffers(
        [serialized_func, serialized_args, serialized_kwargs]
    )


def double(x: int) -> int:
    return x * 2


def slow_double(x: int, sleep_duration_s: int) -> int:
    import time

    time.sleep(sleep_duration_s)
    return x * 2


def kill_manager():
    import os
    import signal

    manager_pid = os.getppid()
    manager_pgid = os.getpgid(manager_pid)
    os.killpg(manager_pgid, signal.SIGKILL)


def div_zero(x: int):
    return x / 0


def succeed_after_n_runs(dirpath: pathlib.Path, fail_count: int = 1):
    import os
    import signal
    from glob import glob

    prior_run_count = len(glob(os.path.join(dirpath, "foo.*.txt")))
    with open(os.path.join(dirpath, f"foo.{prior_run_count+1}.txt"), "w+") as f:
        f.write(f"Hello at {time} counter={prior_run_count+1}")

    if prior_run_count < fail_count:
        manager_pid = os.getppid()
        manager_pgid = os.getpgid(manager_pid)
        os.killpg(manager_pgid, signal.SIGKILL)

    return f"Success on attempt: {prior_run_count+1}"


def get_env_vars():
    import os

    return os.environ
