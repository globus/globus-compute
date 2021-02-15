from funcx.sdk.client import FuncXClient
import pytest
import time


def hello_world() -> str:
    return 'Hello World'


@pytest.mark.skip('Pending github funcx issue: #329')
def test_invalid_function(fxc, endpoint):
    # Assert here that an InvalidFunction exception is raised
    fxc.run(endpoint_id=endpoint,
            function_id='BAD-BAD-BAD-BAD')
