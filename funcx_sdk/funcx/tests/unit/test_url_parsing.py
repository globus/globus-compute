import pytest

from funcx.utils.errors import InvalidServiceAddress
from funcx.utils.url_parsing import (
    validate_service_address,
    ws_uri_from_service_address,
)


def test_validate_service_address_invalid_port():
    url = "https://api.dev.funcx.org:abc/v2"
    with pytest.raises(InvalidServiceAddress, match="Address is malformed"):
        validate_service_address(url)


def test_validate_service_address_invalid_protocol():
    url = "ws://api.dev.funcx.org/v2"
    with pytest.raises(InvalidServiceAddress, match="Protocol must be HTTP/HTTPS"):
        validate_service_address(url)


def test_validate_service_address_address_malformed():
    url = "http:/api.dev.funcx.org/v2"
    with pytest.raises(InvalidServiceAddress, match="Address is malformed"):
        validate_service_address(url)


def test_ws_uri_from_service_address_testing():
    # should expect port to switch from 5000 to 6000
    url = "http://localhost:5000/v2"
    res = ws_uri_from_service_address(url)
    assert res == "ws://localhost:6000/ws/v2/"


def test_ws_uri_from_service_address_dev():
    url = "https://api.dev.funcx.org/v2"
    res = ws_uri_from_service_address(url)
    assert res == "wss://api.dev.funcx.org/ws/v2/"


def test_ws_uri_from_service_address_prod():
    url = "https://api2.funcx.org/v2"
    res = ws_uri_from_service_address(url)
    assert res == "wss://api2.funcx.org/ws/v2/"


def test_ws_uri_from_service_address_other_port():
    # should expect port to remain the same
    url = "https://api.dev.funcx.org:5001/v2"
    res = ws_uri_from_service_address(url)
    assert res == "wss://api.dev.funcx.org:5001/ws/v2/"
