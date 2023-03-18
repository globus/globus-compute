from __future__ import annotations

import random
import string
import time
import uuid

import globus_compute_sdk as gc
import globus_sdk
import pytest
import responses
from globus_compute_sdk.sdk.web_client import WebClient


@pytest.fixture(scope="session")
def endpoint_uuid():
    return str(uuid.UUID(int=0))


@pytest.fixture(scope="session")
def default_endpoint_id():
    return str(uuid.UUID(int=1))


@pytest.fixture(scope="session")
def other_endpoint_id():
    return str(uuid.UUID(int=2))


@pytest.fixture(scope="session")
def tod_session_num():
    yield round(time.time()) % 86400


class FakeLoginManager:
    def ensure_logged_in(self) -> None:
        ...

    def logout(self) -> bool:
        ...

    def get_auth_client(self) -> globus_sdk.AuthClient:
        return globus_sdk.AuthClient(authorizer=globus_sdk.NullAuthorizer())

    def get_search_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient(authorizer=globus_sdk.NullAuthorizer())

    def get_web_client(self, *, base_url: str | None = None) -> WebClient:
        return WebClient(
            base_url="https://api2.funcx.org/v2/",
            authorizer=globus_sdk.NullAuthorizer(),
        )


@pytest.fixture
def get_standard_compute_client():
    responses.add(
        method=responses.GET,
        url="https://api2.funcx.org/v2/version",
        headers={"Content-Type": "application/json"},
        json={"api": "0.4.0", "min_ep_version": "0.0.0", "min_sdk_version": "0.0.0"},
    )

    def func():
        return gc.Client(
            login_manager=FakeLoginManager(),
            do_version_check=False,
        )

    return func


@pytest.fixture
def randomstring():
    def func(length=5, alphabet=string.ascii_letters):
        return "".join(random.choice(alphabet) for _ in range(length))

    return func


@pytest.fixture
def noop():
    def _wrapped(*args, **kwargs):
        pass

    return _wrapped
