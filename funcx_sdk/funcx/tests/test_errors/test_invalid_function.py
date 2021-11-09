import pytest

from funcx.utils.response_errors import FunctionNotFound


def test_invalid_function(fxc, endpoint):
    with pytest.raises(
        FunctionNotFound, match="Function BAD-BAD-BAD-BAD could not be resolved"
    ):
        fxc.run(endpoint_id=endpoint, function_id="BAD-BAD-BAD-BAD")


def test_invalid_function_batch(fxc, endpoint):
    batch = fxc.create_batch()
    batch.add(endpoint_id=endpoint, function_id="BAD-BAD-BAD-BAD")
    with pytest.raises(
        FunctionNotFound, match="Function BAD-BAD-BAD-BAD could not be resolved"
    ):
        fxc.batch_run(batch)
