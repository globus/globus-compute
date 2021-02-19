from funcx.sdk.client import FuncXClient
from funcx.utils.response_errors import FunctionNotFound
import pytest
import time


def test_invalid_function(fxc, endpoint):
    with pytest.raises(FunctionNotFound, match="Function BAD-BAD-BAD-BAD could not be resolved"):
        fxc.run(endpoint_id=endpoint,
                function_id='BAD-BAD-BAD-BAD')
