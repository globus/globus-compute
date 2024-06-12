from __future__ import annotations

import os
import pathlib
from urllib.parse import urlparse

from .utils import get_env_var_with_deprecation

# Same UUID for tutorial MEP in all environments
TUTORIAL_EP_UUID = "4b116d3c-1703-4f8f-9f6f-39921e5864df"
# EP UUID to use while testing, (to be removed)
TEST_EP_UUID = "da6b2745-0dd2-4175-8e11-e35371561165"


def _get_envname():
    return get_env_var_with_deprecation(
        "GLOBUS_SDK_ENVIRONMENT", "FUNCX_SDK_ENVIRONMENT", "production"
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


def _home() -> pathlib.Path:
    # this is a hook point for tests to patch over
    # it just returns `pathlib.Path.home()`
    # replace this with a mock to return some test directory
    return pathlib.Path.home()


def ensure_compute_dir(home: os.PathLike | None = None) -> pathlib.Path:
    dirname = (home if home else _home()) / ".globus_compute"

    user_dir = os.getenv("GLOBUS_COMPUTE_USER_DIR")
    if user_dir:
        dirname = pathlib.Path(user_dir)

    if dirname.is_dir():
        pass
    elif dirname.is_file():
        raise FileExistsError(
            f"Error creating directory {dirname}, "
            "please remove or rename the conflicting file"
        )
    else:
        dirname.mkdir(mode=0o700, parents=True, exist_ok=True)

    return dirname
