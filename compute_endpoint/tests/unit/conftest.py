import functools
import inspect
import os
import pathlib
import random
import typing as t
import uuid

import pytest
from globus_compute_endpoint.engines.helper import execute_task
from tests.conftest import randomstring_impl


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "no_mock_pim: In test_endpointmanager_unit, disable autouse fixture"
    )


known_user_config_opts = {
    "display_name": str,
    "allowed_functions": t.Iterable[uuid.UUID],
    "authentication_policy": uuid.UUID,
    "subscription_id": uuid.UUID,
    "amqp_port": int,
    "heartbeat_period": int,
    "debug": True,
    "heartbeat_threshold": int,
    "idle_heartbeats_soft": int,
    "idle_heartbeats_hard": int,
    "detach_endpoint": False,
    "endpoint_setup": str,
    "endpoint_teardown": str,
    "log_dir": str,
    "stdout": str,
    "stderr": str,
    "local_compute_services": True,
    "environment": str,
    "multi_user": False,
    "executors": None,
}

known_manager_config_opts = {
    "display_name": str,
    "allowed_functions": t.Iterable[uuid.UUID],
    "authentication_policy": uuid.UUID,
    "subscription_id": uuid.UUID,
    "amqp_port": int,
    "heartbeat_period": int,
    "debug": True,
    "public": True,
    "identity_mapping_config_path": os.PathLike,
    "force_mu_allow_same_user": True,
    "mu_child_ep_grace_period_s": float,
    "local_compute_services": True,
    "environment": str,
    "multi_user": True,
}


def get_random_of_datatype_impl(cls):
    if cls == t.Iterable[uuid.UUID]:
        return tuple(str(uuid.uuid4()) for _ in range(random.randint(1, 10)))
    if not inspect.isclass(cls):
        return cls  # not a type; test knows what it needs so just return it
    elif issubclass(cls, uuid.UUID):
        return str(uuid.uuid4())
    elif issubclass(cls, os.PathLike):
        # use an invalid path to guarantee test is run under fs fixture
        p = pathlib.Path("/asadf/asdf/fake/filesystem/dir")
        p.mkdir(parents=True)
        p = p / "Some Test File"
        p.touch()
        return str(p)
    elif issubclass(cls, str):
        return randomstring_impl()
    elif issubclass(cls, bool):
        return random.choice((True, False))
    elif issubclass(cls, int):
        return random.randint(10_000, 1_000_000)
    elif issubclass(cls, float):
        return random.random() * 1_000_000

    raise NotImplementedError(f"Missing test branch for type: {repr(cls)}")


@pytest.fixture
def get_random_of_datatype():
    return get_random_of_datatype_impl


@pytest.fixture
def execute_task_runner(task_uuid, tmp_path):
    return functools.partial(execute_task, task_uuid, run_dir=tmp_path)
