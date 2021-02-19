from funcx.sdk.client import FuncXClient
from funcx.utils.response_errors import EndpointNotFound
import pytest


def hello_world() -> str:
    return 'Hello World'


def test_invalid_endpoint(fxc, endpoint):
    fn_uuid = fxc.register_function(hello_world, endpoint, description='Hello')

    with pytest.raises(EndpointNotFound, match="Endpoint BAD-BAD-BAD-BAD could not be resolved"):
        fxc.run(endpoint_id='BAD-BAD-BAD-BAD',
                function_id=fn_uuid)
