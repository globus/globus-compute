import uuid

import pytest


@pytest.fixture
def endpoint_uuid():
    return str(uuid.UUID(int=0))
