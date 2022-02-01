import time

import pytest
from shared import simple_function, wait_for_task

from funcx.sdk.utils.throttling import MaxRequestsExceeded


@pytest.mark.parametrize("task_count", [100])
def test_bag_of_tasks_fail_REST(fxc, endpoint, task_count):
    """Launches a parametrized bag of tasks using the REST interface.
    This tests for the MaxRequestsExceeded error
    """

    task_ids = []
    fn_uuid = fxc.register_function(simple_function, endpoint)

    with pytest.raises(MaxRequestsExceeded):
        for _i in range(task_count):
            task_id = fxc.run(
                input_data=bytearray(10),
                output_size=10,
                sleep_dur=0,
                function_id=fn_uuid,
                endpoint_id=endpoint,
            )
            task_ids.append(task_id)

    for tid in task_ids:
        x = wait_for_task(fxc, tid, walltime=10)
        assert x == (10, bytearray(10)), f"Result does not match, got: {x}"


@pytest.mark.parametrize("task_count", [100])
def test_bag_of_tasks_REST(fxc, endpoint, task_count):
    """Launches a parametrized bag of tasks using the REST interface.
    Minimal data IO.
    """
    task_ids = []
    fn_uuid = fxc.register_function(simple_function, endpoint)

    for _i in range(task_count):
        task_id = fxc.run(
            input_data=bytearray(10),
            output_size=10,
            sleep_dur=0,
            function_id=fn_uuid,
            endpoint_id=endpoint,
        )
        task_ids.append(task_id)
        time.sleep(1)

    for tid in task_ids:
        x = wait_for_task(fxc, tid, walltime=10)
        assert x == (10, bytearray(10)), f"Result does not match, got: {x}"
