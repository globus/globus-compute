import os
import pathlib
from unittest import mock

import pytest
from globus_compute_sdk.sdk._environments import (
    ensure_compute_dir,
    get_amqp_service_host,
    get_web_service_url,
)


@pytest.fixture(autouse=True)
def _clear_sdk_env(monkeypatch):
    monkeypatch.delenv("FUNCX_SDK_ENVIRONMENT", raising=False)
    monkeypatch.delenv("GLOBUS_SDK_ENVIRONMENT", raising=False)


def test_web_service_url(monkeypatch):
    env_url_map = {
        None: "https://compute.api.globus.org",
        "production": "https://compute.api.globus.org",
        "bad-env-name": "https://compute.api.globus.org",
        "sandbox": "https://compute.api.sandbox.globuscs.info",
        "test": "https://compute.api.test.globuscs.info",
        "preview": "https://compute.api.preview.globus.org",
    }

    for env, url in env_url_map.items():
        assert get_web_service_url(env) == url

    monkeypatch.setenv("FUNCX_SDK_ENVIRONMENT", "dev")
    assert get_web_service_url(None) == "https://api.dev.funcx.org"

    # GLOBUS_SDK_ENVIRONMENT should override FUNCX_SDK_ENVIRONMENT
    monkeypatch.setenv("GLOBUS_SDK_ENVIRONMENT", "sandbox")
    assert get_web_service_url(None) == env_url_map["sandbox"]


def test_get_amqp_service_host(monkeypatch):
    env_url_map = {
        None: "compute.amqps.globus.org",
        "production": "compute.amqps.globus.org",
        "sandbox": "compute.amqps.sandbox.globuscs.info",
        "invalid-env": "compute.amqps.globus.org",
    }

    for env, url in env_url_map.items():
        assert get_amqp_service_host(env) == url

    monkeypatch.setenv("FUNCX_SDK_ENVIRONMENT", "production")
    assert get_amqp_service_host(None) == env_url_map["production"]

    # GLOBUS_SDK_ENVIRONMENT should override FUNCX_SDK_ENVIRONMENT
    monkeypatch.setenv("GLOBUS_SDK_ENVIRONMENT", "sandbox")
    assert get_amqp_service_host(None) == env_url_map["sandbox"]


@pytest.mark.parametrize("dir_exists", [True, False])
@pytest.mark.parametrize("user_dir", ["/my/dir", None, ""])
def test_ensure_compute_dir(fs, dir_exists, user_dir):
    home_dirname = pathlib.Path.home()
    dirname = home_dirname / ".globus_compute"

    if dir_exists:
        fs.create_dir(dirname)

    if user_dir is not None:
        dirname = pathlib.Path(user_dir)
        with mock.patch.dict(os.environ, {"GLOBUS_COMPUTE_USER_DIR": str(dirname)}):
            compute_dirname = ensure_compute_dir()
    else:
        compute_dirname = ensure_compute_dir()

    assert compute_dirname.is_dir()
    assert compute_dirname == dirname
