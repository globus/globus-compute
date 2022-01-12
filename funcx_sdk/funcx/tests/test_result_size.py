import pytest

from funcx.utils.errors import MaxResultSizeExceeded, TaskPending


def large_result_producer(size) -> str:
    return bytearray(size)


def wait_for_task(fxc, task_id, walltime: int = 2):
    import time

    start = time.time()
    while True:
        if time.time() > start + walltime:
            raise Exception("Timeout")
        try:
            r = fxc.get_result(task_id)
        except TaskPending:
            print("Not available yet")
            time.sleep(1)
        except Exception as e:
            raise e
        else:
            return r


test_cases = [4500, 45000, 35000]


@pytest.mark.parametrize("size", test_cases)
def test_allowed_result_sizes(fxc, endpoint, size):
    """funcX should allow all listed result sizes which are under 512KB limit"""
    fn_uuid = fxc.register_function(
        large_result_producer, endpoint, description="LargeResultProducer"
    )
    task_id = fxc.run(
        size,  # This is the current result size limit
        endpoint_id=endpoint,
        function_id=fn_uuid,
    )

    x = wait_for_task(fxc, task_id, walltime=10)
    assert len(x) == size, "Result size does not match excepted size"


def test_result_size_too_large(fxc, endpoint, size=11 * 1024 * 1024):
    """
    funcX should raise a MaxResultSizeExceeded exception when results exceeds 10MB
    limit
    """
    fn_uuid = fxc.register_function(
        large_result_producer, endpoint, description="LargeResultProducer"
    )
    task_id = fxc.run(
        size,  # This is the current result size limit
        endpoint_id=endpoint,
        function_id=fn_uuid,
    )

    with pytest.raises(MaxResultSizeExceeded):
        wait_for_task(fxc, task_id, walltime=10)
