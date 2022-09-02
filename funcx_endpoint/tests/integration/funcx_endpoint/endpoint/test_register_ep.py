import json
import os
from unittest import mock

import pika
import pytest

import funcx
from funcx_endpoint.endpoint.register_endpoint import register_endpoint

ENDPOINT_UUID = "c65f076d-d731-406a-bb55-137faef153b8"


@pytest.mark.skip(
    reason="register won't work until we have figured out the correct data to mock"
)
def test_register_ep(tmp_path):
    endpoint_name = "endpoint_foo"
    fxc = funcx.FuncXClient(login_manager=mock.Mock())
    json_dir = str(tmp_path)
    reg_info = register_endpoint(
        fxc,
        endpoint_uuid=ENDPOINT_UUID,
        endpoint_dir=json_dir,
        endpoint_name=endpoint_name,
    )
    assert isinstance(reg_info, pika.URLParameters)

    assert os.path.exists(tmp_path / "endpoint.json")

    with open(tmp_path / "endpoint.json") as f:
        data = json.load(f)
        assert data["endpoint_id"] == ENDPOINT_UUID
        assert data["endpoint_name"] == endpoint_name
