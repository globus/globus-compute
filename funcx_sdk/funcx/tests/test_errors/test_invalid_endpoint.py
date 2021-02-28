from funcx.sdk.client import FuncXClient
from funcx.utils.response_errors import EndpointNotFound
import pytest


def hello_world() -> str:
    return 'Hello World'


def test_invalid_endpoint(fxc, endpoint):
    fn_uuid = fxc.register_function(hello_world, endpoint, description='Hello')

    # since fxc.run only handles 1 task, it will raise an exception when
    # that task fails to submit
    with pytest.raises(EndpointNotFound, match="Endpoint BAD-BAD-BAD-BAD could not be resolved"):
        fxc.run(endpoint_id='BAD-BAD-BAD-BAD', function_id=fn_uuid)


def test_invalid_endpoint_batch(fxc, endpoint):
    fn_uuid = fxc.register_function(hello_world, endpoint, description='Hello')

    batch = fxc.create_batch()
    batch.add(endpoint_id='BAD-BAD-BAD-BAD', function_id=fn_uuid)
    task_ids = fxc.batch_run(batch)
    # since fxc.batch_run handles many tasks, an exception for a failed task
    # submission only gets raised when the result is checked with fxc.get_result
    with pytest.raises(EndpointNotFound, match="Endpoint BAD-BAD-BAD-BAD could not be resolved"):
        fxc.get_result(task_ids[0])
