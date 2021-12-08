import pytest

from funcx_endpoint.executors.high_throughput.funcx_worker import MaxResultSizeExceeded


def large_result_producer(size: int) -> str:
    return bytearray(size)


def large_arg_consumer(data: str) -> int:
    return len(data)


@pytest.mark.parametrize("size", [200, 2000, 20000, 200000])
def test_allowed_result_sizes(submit_function_and_get_result, endpoint, size):
    """funcX should allow all listed result sizes which are under 512KB limit"""

    r = submit_function_and_get_result(
        endpoint, func=large_result_producer, func_args=(size,)
    )
    assert len(r.result) == size


def test_result_size_too_large(submit_function_and_get_result, endpoint):
    """
    funcX should raise a MaxResultSizeExceeded exception when results exceeds 512KB
    limit
    """
    r = submit_function_and_get_result(
        endpoint, func=large_result_producer, func_args=(550000,)
    )
    assert r.result is None
    assert "exception" in r.response
    # the exception that comes back is a wrapper, so we must "reraise()" to get the
    # true error out
    with pytest.raises(MaxResultSizeExceeded):
        r.response["exception"].reraise()


@pytest.mark.parametrize("size", [200, 2000, 20000, 200000])
def test_allowed_arg_sizes(submit_function_and_get_result, endpoint, size):
    """funcX should allow all listed result sizes which are under 512KB limit"""
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
