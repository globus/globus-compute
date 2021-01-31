from funcx.sdk.client import FuncXClient
import pytest


def hello_world() -> str:
    return 'Hello World'


def test_invalid_endpoint(fxc, endpoint):
    fn_uuid = fxc.register_function(hello_world, endpoint, description='Hello')

    # TODO: currently a generic Exception is raised when the service reports an error,
    # but eventually the service should send an error code and the SDK should raise
    # a corresponding Exception type, such as InvalidEndpoint
    with pytest.raises(Exception, match="Endpoint BAD-BAD-BAD-BAD could not be resolved"):
        fxc.run(endpoint_id='BAD-BAD-BAD-BAD',
                function_id=fn_uuid)
