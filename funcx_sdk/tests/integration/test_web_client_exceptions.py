import pytest
import responses

import funcx
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


@pytest.mark.parametrize("http_status", [400, 500])
@pytest.mark.parametrize(
    "code_int, code_str",
    [
        (1, "user_unauthenticated"),
        (2, "user_not_found"),
        (3, "function_not_found"),
        (4, "endpoint_not_found"),
        (5, "container_not_found"),
        (6, "task_not_found"),
        (7, "auth_group_not_found"),
        (8, "function_access_forbidden"),
        (9, "endpoint_access_forbidden"),
        (10, "function_not_permitted"),
        (11, "endpoint_already_registered"),
        (12, "forwarder_registration_error"),
        (13, "forwarder_contact_error"),
        (14, "endpoint_stats_error"),
        (15, "liveness_stats_error"),
        (16, "request_key_error"),
        (17, "request_malformed"),
        (18, "internal_error"),
        (19, "endpoint_outdated"),
        (20, "task_group_not_found"),
        (21, "task_group_access_forbidden"),
        (22, "invalid_uuid"),
        # any unrecognized code becomes "UNKNOWN"
        (-100, "UNKNOWN"),
    ],
)
def test_code_name_unpacking(client, code_int, code_str, http_status):
    responses.add(
        responses.GET,
        "https://api.funcx/foo",
        json={"code": code_int},
        match_querystring=None,
        status=http_status,
    )
    with pytest.raises(funcx.FuncxAPIError) as excinfo:
        client.get("foo")

    err = excinfo.value
    assert err.code_name == code_str
    assert err.http_status == http_status


@pytest.mark.parametrize("http_status", [400, 500])
def test_reason_parsed_as_part_of_error(client, http_status):
    reason = "you are bad and you should feel bad"
    responses.add(
        responses.GET,
        "https://api.funcx/foo",
        json={"code": 100, "reason": reason},
        match_querystring=None,
        status=http_status,
    )
    with pytest.raises(funcx.FuncxAPIError) as excinfo:
        client.get("foo")

    err = excinfo.value
    assert err.http_status == http_status
    assert err.message == reason
    # the message should be visible in the str form of the error
    assert reason in str(err)
