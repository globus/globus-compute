from funcx.sdk.client import FuncXClient
import pytest
import time

def hello_world() -> str:
    return 'Hello World'

@pytest.mark.skip('Pending github funcx issue: #329')
def test_invalid_endpoint(fxc, endpoint):
    fn_uuid = fxc.register_function(hello_world, endpoint, description='Hello')

    # Assert here that an InvalidEndpoint exception is raised
    task_id = fxc.run(endpoint_id='BAD-BAD-BAD-BAD',
                      function_id=fn_uuid)
