from funcx.sdk.client import FuncXClient
from funcx.utils.response_errors import FunctionNotFound
import pytest
import time


def test_invalid_function(fxc, endpoint):
    # since fxc.run only handles 1 task, it will raise an exception when
    # that task fails to submit
    with pytest.raises(FunctionNotFound, match="Function BAD-BAD-BAD-BAD could not be resolved"):
        fxc.run(endpoint_id=endpoint, function_id='BAD-BAD-BAD-BAD')


def test_invalid_function_batch(fxc, endpoint):
    batch = fxc.create_batch()
    batch.add(endpoint_id=endpoint, function_id='BAD-BAD-BAD-BAD')
    task_ids = fxc.batch_run(batch)
    # since fxc.batch_run handles many tasks, an exception for a failed task
    # submission only gets raised when the result is checked with fxc.get_result
    with pytest.raises(FunctionNotFound, match="Function BAD-BAD-BAD-BAD could not be resolved"):
        fxc.get_result(task_ids[0])
