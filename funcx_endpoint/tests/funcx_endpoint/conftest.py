from __future__ import annotations

import pytest
import responses

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
