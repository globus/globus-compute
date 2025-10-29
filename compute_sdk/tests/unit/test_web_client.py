from __future__ import annotations

import json
import typing as t
import uuid

import pytest
import responses
from globus_compute_sdk.sdk.client import Client
from globus_compute_sdk.sdk.web_client import WebClient
from globus_compute_sdk.version import __version__
from globus_sdk.transport import RequestsTransport

_BASE_URL = "https://api.gc"


@pytest.fixture(autouse=True)
def mocked_responses():
    """
    All tests enable `responses` patching of the `requests` package, replacing
    all HTTP calls.
    """
    responses.start()

    yield

    responses.stop()
    responses.reset()


@pytest.fixture
def client():
    # for the default test client, set a fake URL and disable retries
    RequestsTransport.MAX_RETRIES = 0
    return WebClient(base_url=_BASE_URL)


def test_web_client_deprecated():
    with pytest.warns(DeprecationWarning) as record:
        WebClient(base_url="blah")
    msg = "'WebClient' class is deprecated"
    assert any(msg in str(r.message) for r in record)


def test_web_client_can_set_explicit_base_url():
    c1 = WebClient(base_url="https://foo.example.com")
    c2 = WebClient(base_url="https://bar.example.com")
    assert c1.base_url == "https://foo.example.com"
    assert c2.base_url == "https://bar.example.com"


@pytest.mark.parametrize("service_param", [None, "foo"])
def test_get_version_service_param(client, service_param):
    # if no `service` argument is being passed, expect "all" (the default)
    # otherwise, expect the service argument
    expect_param = service_param if service_param is not None else "all"

    # register the response
    #   {"version": 100}
    # to match on querystring and URL
    responses.add(
        responses.GET,
        f"{_BASE_URL}/v2/version",
        json={"version": 100},
        match=[responses.matchers.query_param_matcher({"service": expect_param})],
    )

    # make the request, ensure the desired response was received
    kwargs = {} if service_param is None else {"service": service_param}
    res = client.get_version(**kwargs)
    assert res["version"] == 100


@pytest.mark.parametrize("app_name", [None, str(uuid.uuid4())])
def test_app_name_value(app_name: str | None):
    wc = WebClient(base_url=_BASE_URL, app_name=app_name)
    if app_name is None:
        assert wc.app_name == f"globus-compute-sdk-{__version__}"
    else:
        assert wc.app_name == app_name


@pytest.mark.parametrize("user_app_name", [None, "baz"])
def test_user_app_name_property(client: WebClient, user_app_name: str | None):
    client.user_app_name = user_app_name
    assert client.user_app_name == client.app_name
    assert f"globus-compute-sdk-{__version__}" in client.app_name
    if user_app_name:
        assert user_app_name in client.app_name


def test_user_app_name_deprecated(client: WebClient):
    with pytest.warns(DeprecationWarning):
        client.user_app_name
    with pytest.warns(DeprecationWarning):
        client.user_app_name = "foo"


def test_get_amqp_url(client, randomstring):
    expected_response = randomstring()
    responses.add(
        responses.GET,
        f"{_BASE_URL}/v2/get_amqp_result_connection_url",
        json={"some_key": expected_response},
    )

    res = client.get_result_amqp_url()
    assert res["some_key"] == expected_response


@pytest.mark.parametrize("multi_user", [None, True, False])
def test_multi_user_post(client: Client, multi_user):
    responses.post(url=f"{_BASE_URL}/v3/endpoints")
    resp = client.register_endpoint("ep_name", None, multi_user=multi_user)
    req_body = json.loads(resp._response.request.body)
    if multi_user:
        assert req_body["multi_user"] == multi_user
    else:
        assert "multi_user" not in req_body


@pytest.mark.parametrize("multi_user", [None, True, False])
def test_multi_user_put(client: Client, multi_user):
    ep_uuid = uuid.uuid4()
    responses.put(url=f"{_BASE_URL}/v3/endpoints/{ep_uuid}")
    resp = client.register_endpoint("ep_name", ep_uuid, multi_user=multi_user)
    req_body = json.loads(resp._response.request.body)
    if multi_user:
        assert req_body["multi_user"] == multi_user
    else:
        assert "multi_user" not in req_body


def test_get_function(client: WebClient, randomstring: t.Callable):
    func_uuid_str = str(uuid.uuid4())
    expected_response = randomstring()
    responses.add(
        responses.GET,
        f"{_BASE_URL}/v2/functions/{func_uuid_str}",
        json={"some_key": expected_response},
    )

    res = client.get_function(func_uuid_str)

    assert res["some_key"] == expected_response


def test_get_allowed_functions(client: WebClient, randomstring: t.Callable):
    ep_uuid_str = str(uuid.uuid4())
    expected_response = randomstring()
    responses.add(
        responses.GET,
        f"{_BASE_URL}/v3/endpoints/{ep_uuid_str}/allowed_functions",
        json={"some_key": expected_response},
    )

    res = client.get_allowed_functions(ep_uuid_str)

    assert res["some_key"] == expected_response


def test_delete_function(client: WebClient, randomstring: t.Callable):
    func_uuid_str = str(uuid.uuid4())
    expected_response = randomstring()
    responses.add(
        responses.DELETE,
        f"{_BASE_URL}/v2/functions/{func_uuid_str}",
        json={"some_key": expected_response},
    )

    res = client.delete_function(func_uuid_str)

    assert res["some_key"] == expected_response


@pytest.mark.parametrize("ep_uuid", [uuid.uuid4(), None])
def test_register_endpoint_post_put(client: WebClient, ep_uuid: t.Optional[uuid.UUID]):
    post_response = {"post": "response"}
    put_response = {"put": "response"}
    responses.add(
        responses.PUT,
        f"{_BASE_URL}/v3/endpoints/{ep_uuid}",
        json=put_response,
    )
    responses.add(
        responses.POST,
        f"{_BASE_URL}/v3/endpoints",
        json=post_response,
    )

    res = client.register_endpoint("MyEP", ep_uuid)

    if ep_uuid:
        assert res.data == put_response
    else:
        assert res.data == post_response
