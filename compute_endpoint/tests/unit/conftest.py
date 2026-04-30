from __future__ import annotations

import functools
import inspect
import os
import pathlib
import random
import typing as t
import uuid
from unittest import mock

import pytest
from globus_compute_endpoint.endpoint import endpoint
from globus_compute_endpoint.endpoint.config import (
    PamConfiguration,
    UserEndpointConfig,
)
from globus_compute_endpoint.endpoint.config.config import PathModel
from globus_compute_endpoint.engines import ThreadPoolEngine
from globus_compute_endpoint.engines.helper import execute_task
from parsl import HighThroughputExecutor
from parsl.executors import MPIExecutor
from tests.conftest import randomstring_impl


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "no_mock_pim: In test_endpointmanager_unit, disable autouse fixture"
    )
    config.addinivalue_line(
        "markers", "no_mock_shutil: In test_endpointmanager_unit, disable autouse"
    )


known_user_config_opts = {
    "display_name": str,
    "allowed_functions": t.Iterable[uuid.UUID],
    "authentication_policy": uuid.UUID,
    "subscription_id": uuid.UUID,
    "admins": t.Iterable[uuid.UUID],
    "amqp_port": int,
    "heartbeat_period": int,
    "debug": True,
    "heartbeat_threshold": int,
    "idle_heartbeats_soft": int,
    "idle_heartbeats_hard": int,
    "endpoint_setup": str,
    "endpoint_teardown": str,
    "local_compute_services": True,
    "environment": str,
    "high_assurance": False,
    "engine": None,
    "paths": None,
}

known_manager_config_opts = {
    "display_name": str,
    "allowed_functions": t.Iterable[uuid.UUID],
    "authentication_policy": uuid.UUID,
    "subscription_id": uuid.UUID,
    "admins": t.Iterable[uuid.UUID],
    "amqp_port": int,
    "heartbeat_period": int,
    "debug": True,
    "public": True,
    "user_config_template_path": os.PathLike,
    "user_config_schema_path": os.PathLike,
    "identity_mapping_config_path": os.PathLike,
    "audit_log_path": os.PathLike,
    "pam": PamConfiguration,
    "mu_child_ep_grace_period_s": float,
    "local_compute_services": True,
    "environment": str,
    "high_assurance": True,
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
        p.mkdir(parents=True, exist_ok=True)
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
    elif issubclass(cls, PamConfiguration):
        return PamConfiguration(True, "some-service-name")
    elif issubclass(cls, PathModel):
        return PathModel(endpoint_log="abc.txt", gc_dir="/some_dir")

    raise NotImplementedError(f"Missing test branch for type: {repr(cls)}")


@pytest.fixture
def get_random_of_datatype():
    return get_random_of_datatype_impl


@pytest.fixture
def execute_task_runner(task_uuid, tmp_path):
    return functools.partial(execute_task, task_id=task_uuid, run_dir=tmp_path)


@pytest.fixture
def mock_htex():
    m = mock.Mock(spec=HighThroughputExecutor, monitoring_messages=None)
    m.status_polling_interval = 5
    m.launch_cmd = "launchy"
    m.interchange_launch_cmd = "ix-launchy"
    return m


@pytest.fixture
def mock_mpiex():
    m = mock.Mock(spec=MPIExecutor, monitoring_messages=None)
    m.status_polling_interval = 5
    m.launch_cmd = "launchy"
    m.interchange_launch_cmd = "ix-launchy"
    return m


@pytest.fixture
def mock_get_config():
    with mock.patch("globus_compute_endpoint.endpoint.endpoint.get_config") as m:
        yield m


@pytest.fixture
def conf():
    _conf = UserEndpointConfig(engine=ThreadPoolEngine())
    _conf.source_content = "# test source content"
    _conf.source_content += "\nengine:\n  type: ThreadPoolEngine"
    yield _conf


@pytest.fixture
def mock_ep_data(fs, conf):
    ep = endpoint.Endpoint()
    ep_dir = pathlib.Path("/some/path/mock_endpoint")
    ep_dir.mkdir(parents=True, exist_ok=True)
    log_to_console = False
    yield ep, ep_dir, log_to_console, conf


@pytest.fixture
def mock_ep_dir(fs, mock_ep_data, mock_get_config):
    ep, ep_dir, *_, conf = mock_ep_data
    mock_get_config.return_value = conf
    ep._config_file_path(ep_dir).write_text(conf.source_content)
    yield ep, ep_dir
