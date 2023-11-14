from __future__ import annotations

import typing as t

import pytest
import responses
from pika.exchange_type import ExchangeType


@pytest.fixture(autouse=True)
def _autouse_responses():
    responses.start()

    yield

    responses.stop()
    responses.reset()


# mock logging config to do nothing
@pytest.fixture(autouse=True)
def _mock_logging_config(monkeypatch):
    def fake_setup_logging(*args, **kwargs):
        pass

    monkeypatch.setattr(
        "globus_compute_endpoint.logging_config.setup_logging", fake_setup_logging
    )


@pytest.fixture
def setup_register_endpoint_response(
    create_result_queue_info,
    task_queue_info,
    ensure_task_queue,
    ensure_result_queue,
) -> t.Callable[[str], None]:
    ensure_task_queue(queue_opts={"queue": task_queue_info["queue"]})

    def resp(endpoint_uuid: str):
        rq_info = create_result_queue_info(queue_id=endpoint_uuid)
        exchange_name = rq_info["exchange"]
        queue_name = rq_info["test_routing_key"]
        exchange_opts = {
            "exchange": exchange_name,
            "exchange_type": ExchangeType.topic.value,
            "durable": True,
        }
        queue_opts = {"queue": queue_name, "durable": True}
        ensure_result_queue(exchange_opts=exchange_opts, queue_opts=queue_opts)

        responses.add(
            method=responses.POST,
            url="https://compute.api.globus.org/v3/endpoints",
            headers={"Content-Type": "application/json"},
            json={
                "endpoint_id": endpoint_uuid,
                "task_queue_info": task_queue_info,
                "result_queue_info": rq_info,
            },
        )

        responses.add(
            method=responses.PUT,
            url=f"https://compute.api.globus.org/v3/endpoints/{endpoint_uuid}",
            headers={"Content-Type": "application/json"},
            json={
                "endpoint_id": endpoint_uuid,
                "task_queue_info": task_queue_info,
                "result_queue_info": rq_info,
            },
        )

    yield resp
