import pytest
import responses

from funcx.sdk.web_client import FuncxWebClient


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
    return FuncxWebClient(
        base_url="https://api.funcx", transport_params={"max_retries": 0}
    )


def test_web_client_can_set_explicit_base_url():
    c1 = FuncxWebClient(base_url="https://foo.example.com/")
    c2 = FuncxWebClient(base_url="https://bar.example.com/")
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
