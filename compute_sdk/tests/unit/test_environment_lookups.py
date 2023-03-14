import pytest
from globus_compute_sdk.sdk._environments import (
    get_web_service_url,
    get_web_socket_url,
    urls_might_mismatch,
)


@pytest.fixture(autouse=True)
def _clear_sdk_env(monkeypatch):
    monkeypatch.delenv("FUNCX_SDK_ENVIRONMENT", raising=False)


def test_web_service_url(monkeypatch):
    assert get_web_service_url(None) == "https://api2.funcx.org/v2"
    assert get_web_service_url("production") == "https://api2.funcx.org/v2"
    assert get_web_service_url("no-such-env-name-known") == "https://api2.funcx.org/v2"
    assert get_web_service_url("dev") == "https://api.dev.funcx.org/v2"
    monkeypatch.setenv("FUNCX_SDK_ENVIRONMENT", "dev")
    assert get_web_service_url(None) == "https://api.dev.funcx.org/v2"


def test_web_socket_url(monkeypatch):
    assert get_web_socket_url(None) == "wss://api2.funcx.org/ws/v2/"
    assert get_web_socket_url("production") == "wss://api2.funcx.org/ws/v2/"
    assert get_web_socket_url("no-such-env-name-known") == "wss://api2.funcx.org/ws/v2/"
    assert get_web_socket_url("dev") == "wss://api.dev.funcx.org/ws/v2/"
    monkeypatch.setenv("FUNCX_SDK_ENVIRONMENT", "dev")
    assert get_web_socket_url(None) == "wss://api.dev.funcx.org/ws/v2/"


@pytest.mark.parametrize(
    "service_url, socket_url, expect_mismatch",
    [
        # matches:
        # prod, prod
        ["https://api2.funcx.org/v2", "wss://api2.funcx.org/ws/v2/", False],
        # dev, dev
        ["https://api.dev.funcx.org/v2", "wss://api.dev.funcx.org/ws/v2/", False],
        # local, local
        ["http://localhost:5000/v2", "ws://localhost:6000/v2", False],
        # mismatches:
        # prod, dev
        ["https://api2.funcx.org/v2", "wss://api.dev.funcx.org/ws/v2/", True],
        # dev, prod
        ["https://api.dev.funcx.org/v2", "wss://api2.funcx.org/ws/v2/", True],
        # local, dev
        ["http://localhost:5000/v2", "wss://api.dev.funcx.org/ws/v2/", True],
        # dev, local
        ["https://api.dev.funcx.org/v2", "ws://localhost:6000/v2/", True],
        # local, prod
        ["http://localhost:5000/v2", "wss://api2.funcx.org/ws/v2/", True],
        # prod, local
        ["https://api2.funcx.org/v2", "ws://localhost:6000/v2/", True],
    ],
)
def test_url_mismatch(service_url, socket_url, expect_mismatch):
    message = f"{service_url} and {socket_url} should "
    message += "mismatch" if expect_mismatch else "match"
    assert urls_might_mismatch(service_url, socket_url) == expect_mismatch, message


@pytest.mark.parametrize(
    "envname", ["dev", "production", "local", "no-such-env-name-known"]
)
def test_built_in_environments_match(envname):
    service_url = get_web_service_url(envname)
    socket_url = get_web_socket_url(envname)
    assert not urls_might_mismatch(service_url, socket_url)
