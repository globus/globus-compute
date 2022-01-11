import pytest

from funcx.sdk._environments import get_web_service_url, get_web_socket_url


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
