from __future__ import annotations

import os
from urllib.parse import urlparse


def _get_envname():
    return os.getenv("FUNCX_SDK_ENVIRONMENT", "production")


def get_web_service_url(envname: str | None) -> str:
    env = envname or _get_envname()
    urls = {
        "production": "https://api2.funcx.org/v2",
        "dev": "https://api.dev.funcx.org/v2",
        "local": "http://localhost:5000/v2",
    }
    return urls.get(env, urls["production"])


def get_web_socket_url(envname: str | None) -> str:
    env = envname or _get_envname()
    urls = {
        "production": "wss://api2.funcx.org/ws/v2/",
        "dev": "wss://api.dev.funcx.org/ws/v2/",
        "local": "ws://localhost:6000/v2",
    }
    return urls.get(env, urls["production"])


def urls_might_mismatch(service_url: str, socket_url: str) -> bool:
    parsed_service, parsed_socket = urlparse(service_url), urlparse(socket_url)
    return parsed_service.hostname != parsed_socket.hostname
