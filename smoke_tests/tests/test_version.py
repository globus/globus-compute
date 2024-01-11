import pytest
from packaging.version import Version


def test_web_service(compute_client, endpoint, compute_test_config):
    """This test checks 1) web-service is online, 2) version of the web-service"""
    response = compute_client.web_client.get_version()

    assert response.http_status == 200, (
        "Request to version expected status_code=200, "
        f"got {response.http_status} instead"
    )

    service_version = response.data["api"]
    api_min_version = compute_test_config.get("api_min_version")
    if api_min_version is not None:
        parsed_min = Version(api_min_version)
        parsed_service = Version(service_version)
        assert (
            parsed_service >= parsed_min
        ), f"Expected API version >={api_min_version}, got {service_version}"


def say_hello():
    return "Hello World!"


def test_simple_function(compute_client):
    """Test whether we can register a function"""
    func_uuid = compute_client.register_function(say_hello)
    assert func_uuid is not None, "Invalid function uuid returned"


@pytest.mark.skip(
    "Skipping as of 2024-01-11 while we wait for MU tutorial to show as 'online'"
)
def test_ep_status(compute_client, endpoint):
    """Test whether the tutorial EP is online and reporting status"""
    response = compute_client.get_endpoint_status(endpoint)

    assert (
        response["status"] == "online"
    ), f"Expected tutorial EP to be online, got:{response['status']}"
