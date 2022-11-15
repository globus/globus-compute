import json
import uuid

import pytest
import responses
from globus_sdk.exc.api import GlobusAPIError

from funcx_endpoint.endpoint.register_endpoint import register_endpoint


@pytest.fixture
def register_endpoint_response(endpoint_uuid):
    def create_response(
        endpoint_id=endpoint_uuid,
        queue_ttl_s=60,
        queue_arguments=None,
        queue_kwargs=None,
        rmq_fqdn="rabbitmq.fqdn",
        username="u",
        password="p",
    ):
        if queue_arguments is None:
            queue_arguments = {"x-expires": queue_ttl_s * 1000}
        if queue_kwargs is None:
            queue_kwargs: dict = {"durable": True, "arguments": queue_arguments}
        creds = ""
        if username and password:
            creds = f"{username}:{password}@"
        responses.add(
            method=responses.POST,
            url="https://api2.funcx.org/v2/endpoints",
            headers={"Content-Type": "application/json"},
            json={
                "endpoint_id": endpoint_id,
                "task_queue_info": {
                    "exchange_name": "tasks",
                    "connection_url": f"amqp://{creds}{rmq_fqdn}",
                    "args": queue_kwargs,
                },
                "result_queue_info": {
                    "exchange_name": "results",
                    "connection_url": f"amqp://{creds}{rmq_fqdn}",
                    "args": queue_kwargs,
                    "routing_key": f"{endpoint_uuid}.results",
                },
            },
        )

    return create_response


@pytest.fixture
def register_endpoint_failure_response(endpoint_uuid):
    def create_response(endpoint_id=endpoint_uuid, status_code=200):
        responses.add(
            method=responses.POST,
            url="https://api2.funcx.org/v2/endpoints",
            headers={"Content-Type": "application/json"},
            json={"error": "error msg"},
            status=status_code,
        )

    return create_response


@responses.activate
def test_register_endpoint_happy_path(
    tmp_path,
    register_endpoint_response,
    get_standard_funcx_client,
    randomstring,
    mocker,
):
    mock_log = mocker.patch("funcx_endpoint.endpoint.register_endpoint.log")
    ep_uuid = str(uuid.uuid4())
    uname, pword = randomstring(), randomstring()
    register_endpoint_response(endpoint_id=ep_uuid, username=uname, password=pword)
    fxc = get_standard_funcx_client()
    ep_name = randomstring()
    tq_info, rq_info = register_endpoint(
        fxc, endpoint_uuid=ep_uuid, endpoint_dir=tmp_path, endpoint_name=ep_name
    )
    ep_json_p = tmp_path / "endpoint.json"
    assert ep_json_p.exists()
    ep_data = json.load(ep_json_p.open())
    assert ep_data["endpoint_name"] == ep_name
    assert ep_data["endpoint_id"] == ep_uuid
    assert uname not in str(ep_data)
    assert pword not in str(ep_data)

    assert "connection_url" in tq_info
    assert "connection_url" in rq_info

    args, kwargs = mock_log.info.call_args
    assert "Registration returned: " in args[0]
    assert uname not in str(args)
    assert pword not in str(args)


@responses.activate
def test_register_endpoint_invalid_response(
    tmp_path,
    endpoint_uuid,
    other_endpoint_id,
    register_endpoint_response,
    get_standard_funcx_client,
):
    register_endpoint_response(endpoint_id=other_endpoint_id)
    fxc = get_standard_funcx_client()
    with pytest.raises(ValueError):
        register_endpoint(
            fxc,
            endpoint_uuid=endpoint_uuid,
            endpoint_dir=tmp_path,
            endpoint_name="Some EP Name",
        )


@responses.activate
def test_register_endpoint_locked_error(
    tmp_path,
    register_endpoint_failure_response,
    get_standard_funcx_client,
):
    """
    Check to ensure endpoint registration escalates up with API error
    """
    ep_uuid = str(uuid.uuid4())
    register_endpoint_failure_response(endpoint_id=ep_uuid, status_code=423)
    fxc = get_standard_funcx_client()
    with pytest.raises(GlobusAPIError):
        register_endpoint(
            fxc, endpoint_uuid=ep_uuid, endpoint_dir=tmp_path, endpoint_name="a"
        )


@pytest.mark.parametrize("multi_tenant", [None, True, False])
@responses.activate
def test_register_endpoint_multi_tenant(
    tmp_path,
    endpoint_uuid,
    register_endpoint_response,
    get_standard_funcx_client,
    randomstring,
    multi_tenant,
):
    ep_uuid = str(uuid.uuid4())
    ep_name = randomstring()

    register_endpoint_response(endpoint_id=ep_uuid)

    fxc = get_standard_funcx_client()
    if multi_tenant is not None:
        register_endpoint(
            fxc,
            endpoint_uuid=ep_uuid,
            endpoint_dir=str(tmp_path),
            endpoint_name=ep_name,
            multi_tenant=multi_tenant,
        )
    else:
        register_endpoint(
            fxc,
            endpoint_uuid=ep_uuid,
            endpoint_dir=str(tmp_path),
            endpoint_name=ep_name,
        )

    ep_json_p = tmp_path / "endpoint.json"
    assert ep_json_p.exists()

    request_body = str(responses.calls[1].request.body)
    if multi_tenant is True:
        assert '"multi_tenant": true' in request_body
    else:
        assert "multi_tenant" not in request_body
