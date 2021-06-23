import argparse
import time

import pytest

from funcx.sdk.client import FuncXClient
from funcx_endpoint.executors.high_throughput.funcx_worker import MaxResultSizeExceeded


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
        except Exception:
            print("Not available yet")
            time.sleep(1)
        else:
            return r


def test_large_result(fxc, endpoint, size=(11 * (2 ** 20))):
    fn_uuid = fxc.register_function(
        large_result_producer, endpoint, description="LargeResultProducer"
    )
    task_id = fxc.run(
        size,  # This is the current result size limit
        endpoint_id=endpoint,
        function_id=fn_uuid,
    )

    print("Task_id: ", task_id)
    # Replace the stupid sleep
    time.sleep(5)
    with pytest.raises(MaxResultSizeExceeded):
        fxc.get_result(task_id)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint_id", required=True)
    args = parser.parse_args()

    fxc = FuncXClient()
    endpoint = args.endpoint_id
    test_large_result(fxc, endpoint, 2 ** 10)
    test_large_result(fxc, endpoint, 11 * (2 ** 20))  # 11 MB exceeds the limit
