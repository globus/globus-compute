import requests
from packaging.version import Version


def test_web_service(gcc, endpoint, compute_test_config):
    """This test checks 1) web-service is online, 2) version of the web-service"""
    service_address = gcc.funcx_service_address

    response = requests.get(f"{service_address}/version")

    assert response.status_code == 200, (
        "Request to version expected status_code=200, "
        f"got {response.status_code} instead"
    )

    service_version = response.json()
    api_min_version = compute_test_config.get("api_min_version")
    if api_min_version is not None:
        parsed_min = Version(api_min_version)
        parsed_service = Version(service_version)
        assert (
            parsed_service >= parsed_min
        ), f"Expected API version >={api_min_version}, got {service_version}"


def say_hello():
    return "Hello World!"


def test_simple_function(gcc):
    """Test whether we can register a function"""
    func_uuid = gcc.register_function(say_hello)
    assert func_uuid is not None, "Invalid function uuid returned"


def test_ep_status(gcc, endpoint):
    """Test whether the tutorial EP is online and reporting status"""
    response = gcc.get_endpoint_status(endpoint)

    assert (
        response["status"] == "online"
    ), f"Expected tutorial EP to be online, got:{response['status']}"
