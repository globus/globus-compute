import pytest
from funcx_common.task_storage.default_storage import DEFAULT_REDIS_STORAGE_THRESHOLD

from funcx.errors import FuncxTaskExecutionFailed


def large_result_producer(size: int) -> str:
    return bytearray(size)


def large_arg_consumer(data: str) -> int:
    return len(data)


@pytest.mark.parametrize("size", [200, 2000, 20000, 200000])
def test_allowed_result_sizes(submit_function_and_get_result, endpoint, size):
    """funcX should allow all listed result sizes which are under 512KB limit"""
    if size >= DEFAULT_REDIS_STORAGE_THRESHOLD:
        pytest.skip(
            "prod currently has a race condition with S3, which has been fixed in dev"
        )

    r = submit_function_and_get_result(
        endpoint, func=large_result_producer, func_args=(size,)
    )
    assert len(r.result) == size


def test_result_size_too_large(submit_function_and_get_result, endpoint):
    """
    funcX should raise a MaxResultSizeExceeded exception when results exceeds 10MB
    limit
    """
    # SDK wraps remote execution failures in FuncxTaskExecutionFailed exceptions...
    with pytest.raises(FuncxTaskExecutionFailed) as excinfo:
        submit_function_and_get_result(
            endpoint, func=large_result_producer, func_args=(11 * 1024 * 1024,)
        )
        # ...so unwrap the exception to verify that it's the right type
        assert "MaxResultSizeExceeded" in excinfo.value.remote_data


@pytest.mark.parametrize("size", [200, 2000, 20000, 200000])
def test_allowed_arg_sizes(submit_function_and_get_result, endpoint, size):
    """funcX should allow all listed result sizes which are under 512KB limit"""
    if size >= DEFAULT_REDIS_STORAGE_THRESHOLD:
        pytest.skip(
            "prod currently has a race condition with S3, which has been fixed in dev"
        )

    r = submit_function_and_get_result(
        endpoint, func=large_arg_consumer, func_args=(bytearray(size),)
    )
    assert r.result == size


@pytest.mark.skip(reason="As of 0.3.4, an arg size limit is not being enforced")
def test_arg_size_too_large(submit_function_and_get_result, endpoint, size=55000000):
    """funcX should raise an exception for objects larger than some limit,
    which we are yet to define. This does not work right now.
    """

    r = submit_function_and_get_result(
        endpoint, func=large_result_producer, func_args=(bytearray(550000),)
    )
    assert r.result is None
    assert "exception" in r.response
