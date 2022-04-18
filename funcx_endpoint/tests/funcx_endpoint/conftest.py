from __future__ import annotations

import globus_sdk
import pytest
import responses

import funcx
from funcx.sdk.web_client import FuncxWebClient
from funcx_endpoint.logging_config import add_trace_level


@pytest.fixture(autouse=True)
def _autouse_responses():
    responses.start()

    yield

    responses.stop()
    responses.reset()


@pytest.fixture(autouse=True, scope="session")
def _set_logging_trace_level():
    add_trace_level()


# mock logging config to do nothing
@pytest.fixture(autouse=True)
def _mock_logging_config(monkeypatch):
    def fake_setup_logging(*args, **kwargs):
        pass

    monkeypatch.setattr(
        "funcx_endpoint.logging_config.setup_logging", fake_setup_logging
    )


class FakeLoginManager:
    def ensure_logged_in(self) -> None:
        ...

    def logout(self) -> None:
        ...

    def get_auth_client(self) -> globus_sdk.AuthClient:
        return globus_sdk.AuthClient(authorizer=globus_sdk.NullAuthorizer())

    def get_search_client(self) -> globus_sdk.SearchClient:
        return globus_sdk.SearchClient(authorizer=globus_sdk.NullAuthorizer())

    def get_funcx_web_client(self, *, base_url: str | None = None) -> FuncxWebClient:
        return FuncxWebClient(
            base_url="https://api2.funcx.org/v2/",
            authorizer=globus_sdk.NullAuthorizer(),
        )


@pytest.fixture
def setup_register_endpoint_response(endpoint_uuid):
    responses.add(
        method=responses.POST,
        url="https://api2.funcx.org/v2/endpoints",
        headers={"Content-Type": "application/json"},
        json={
            "endpoint_id": endpoint_uuid,
            "result_queue_info": {
                "exchange_name": "results",
                "exchange_type": "topic",
                "result_url": "amqp://localhost:5672",
            },
            "task_queue_info": {
                "exchange_name": "tasks",
                "exchange_type": "direct",
                "task_url": "amqp://localhost:5672",
            },
        },
    )


@pytest.fixture
def get_standard_funcx_client():
    responses.add(
        method=responses.GET,
        url="https://api2.funcx.org/v2/version",
        headers={"Content-Type": "application/json"},
        json={"api": "0.4.0", "min_ep_version": "0.0.0", "min_sdk_version": "0.0.0"},
    )

    def func():
        return funcx.FuncXClient(
            use_offprocess_checker=False,
            login_manager=FakeLoginManager(),
            do_version_check=False,
        )

    return func
