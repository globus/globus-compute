from __future__ import annotations

import os
from urllib.parse import urlparse


def _get_envname():
    return os.getenv(
        "GLOBUS_SDK_ENVIRONMENT",
        # Fallback to the old ENV var if GLOBUS_SDK_ENVIRONMENT is not set
        os.getenv("FUNCX_SDK_ENVIRONMENT", "production"),
    )


def get_web_service_url(envname: str | None = None) -> str:
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


def get_amqp_service_host(env_name: str | None = None) -> str:
    env = env_name or _get_envname()
    hosts = {
        "production": "compute.amqps.globus.org",
        "dev": "amqps.dev.funcx.org",
        "preview": "compute.amqps.preview.globus.org",
        "local": "localhost",
    }
    for test_env in ["sandbox", "test", "integration", "staging"]:
        hosts[test_env] = f"compute.amqps.{test_env}.globuscs.info"

    return hosts.get(env, hosts["production"])


def remove_url_path(url: str):
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}"
