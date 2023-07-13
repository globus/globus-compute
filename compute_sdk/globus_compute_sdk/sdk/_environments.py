from __future__ import annotations

import os
from urllib.parse import urlparse


def _get_envname():
    return os.getenv(
        "GLOBUS_SDK_ENVIRONMENT",
        # Fallback to the old ENV var if GLOBUS_SDK_ENVIRONMENT is not set
        os.getenv("FUNCX_SDK_ENVIRONMENT", "production"),
    )


def get_web_service_url(envname: str | None) -> str:
    env = envname or _get_envname()
    urls = {
        "production": "https://compute.api.globus.org",
        # Special case for preview unlike the other globuscs.info envs
        "preview": "https://compute.api.preview.globus.org",
        "dev": "https://api.dev.funcx.org",
        "local": "http://localhost:5000",
    }
    for test_env in ["sandbox", "test", "integration", "staging"]:
        urls[test_env] = f"https://compute.api.{test_env}.globuscs.info"

    return urls.get(env, urls["production"])


def get_web_socket_url(envname: str | None) -> str:
    env = envname or _get_envname()
    urls = {
        "production": "wss://compute.api.globus.org/ws/v2/",
        "dev": "wss://api.dev.funcx.org/ws/v2/",
        "local": "ws://localhost:6000/v2",
    }
    return urls.get(env, urls["production"])


def urls_might_mismatch(service_url: str, socket_url: str) -> bool:
    parsed_service, parsed_socket = urlparse(service_url), urlparse(socket_url)
    return parsed_service.hostname != parsed_socket.hostname


def remove_url_path(url: str):
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}"
