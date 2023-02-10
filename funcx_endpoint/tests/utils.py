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


def ez_pack_function(serializer, func, args, kwargs):
    serialized_func = serializer.serialize(func)
    serialized_args = serializer.serialize(args)
    serialized_kwargs = serializer.serialize(kwargs)
    return serializer.pack_buffers(
        [serialized_func, serialized_args, serialized_kwargs]
    )


def double(x: int) -> int:
    return x * 2
