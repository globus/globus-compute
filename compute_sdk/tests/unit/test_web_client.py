import json

import pytest
import responses
from globus_compute_sdk.sdk.web_client import WebClient
from globus_compute_sdk.version import __version__


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
    return WebClient(base_url="https://api.funcx", transport_params={"max_retries": 0})


def test_web_client_can_set_explicit_base_url():
    c1 = WebClient(base_url="https://foo.example.com/")
    c2 = WebClient(base_url="https://bar.example.com/")
    assert c1.base_url == "https://foo.example.com/"
    assert c2.base_url == "https://bar.example.com/"


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
        "https://api.funcx/version",
        json={"version": 100},
        match=[responses.matchers.query_param_matcher({"service": expect_param})],
    )

    # make the request, ensure the desired response was received
    kwargs = {} if service_param is None else {"service": service_param}
    res = client.get_version(**kwargs)
    assert res["version"] == 100


@pytest.mark.parametrize("user_app_name", [None, "bar"])
def test_app_name_from_constructor(user_app_name):
    client = WebClient(
        # use the same fake URL and disable retries as in the default test case
        base_url="https://api.funcx",
        transport_params={"max_retries": 0},
        # and also pass in the app_name
        app_name=user_app_name,
    )

    assert client.user_app_name == user_app_name
    assert __version__ in client.app_name
    assert "globus-compute-sdk" in client.app_name
    if user_app_name:
        assert user_app_name in client.app_name


@pytest.mark.parametrize("user_app_name", [None, "baz"])
def test_user_app_name_property(client, user_app_name):
    client.user_app_name = user_app_name

    assert client.user_app_name == user_app_name
    assert __version__ in client.app_name
    assert "globus-compute-sdk" in client.app_name
    if user_app_name:
        assert user_app_name in client.app_name


def test_app_name_not_settable(client):
    with pytest.raises(NotImplementedError):
        client.app_name = "qux"


def test_get_amqp_url(client, randomstring):
    expected_response = randomstring()
    responses.add(
        responses.GET,
        "https://api.funcx/get_amqp_result_connection_url",
        json={"some_key": expected_response},
    )

    res = client.get_result_amqp_url()
    assert res["some_key"] == expected_response


@pytest.mark.parametrize("multi_tenant", [None, True, False])
def test_multi_tenant_post(client, multi_tenant):
    responses.post(url="https://api.funcx/endpoints")
    resp = client.register_endpoint("ep_name", "ep_id", multi_tenant=multi_tenant)
    req_body = json.loads(resp._response.request.body)
    if multi_tenant:
        assert req_body["multi_tenant"] == multi_tenant
    else:
        assert "multi_tenant" not in req_body
