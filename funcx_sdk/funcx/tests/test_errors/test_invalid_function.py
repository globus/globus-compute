from funcx.sdk.client import FuncXClient
import pytest
import time


def hello_world() -> str:
    return 'Hello World'


def test_invalid_function(fxc, endpoint):
    # TODO: currently a generic Exception is raised when the service reports an error,
    # but eventually the service should send an error code and the SDK should raise
    # a corresponding Exception type, such as InvalidEndpoint
    with pytest.raises(Exception, match="Function BAD-BAD-BAD-BAD could not be resolved"):
        fxc.run(endpoint_id=endpoint,
                function_id='BAD-BAD-BAD-BAD')
