import pytest
import responses
from globus_compute_sdk.sdk.web_client import WebClient
from globus_sdk.exc.api import GlobusAPIError
from globus_sdk.transport import RequestsTransport


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
    # for the default test client, set a fake URL
    return WebClient(base_url="https://api.funcx")


@pytest.mark.parametrize("http_status", [400, 500])
def test_message_parsed_as_part_of_error(client, http_status):
    # we'll need to set retries to 0, previously possible in web_client init
    retries = RequestsTransport.DEFAULT_MAX_RETRIES
    RequestsTransport.DEFAULT_MAX_RETRIES = 0

    message = "you are bad and you should feel bad"
    responses.add(
        responses.GET,
        "https://api.funcx/foo",
        json={"code": 100, "message": message},
        match_querystring=None,
        status=http_status,
    )
    with pytest.raises(GlobusAPIError) as excinfo:
        client.get("foo")

    # Put things back the way they were
    RequestsTransport.DEFAULT_MAX_RETRIES = retries

    err = excinfo.value
    assert err.http_status == http_status
    assert message in err.message
    # the message should be visible in the str form of the error
    assert message in str(err)
