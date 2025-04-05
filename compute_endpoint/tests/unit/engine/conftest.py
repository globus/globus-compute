from unittest import mock

import pytest
from parsl.executors import HighThroughputExecutor, MPIExecutor


@pytest.fixture
def mock_htex():
    m = mock.Mock(spec=HighThroughputExecutor)
    m.status_polling_interval = 5
    m.launch_cmd = "launchy"
    m.interchange_launch_cmd = "ix-launchy"
    return m


@pytest.fixture
def mock_mpiex():
    m = mock.Mock(spec=MPIExecutor)
    m.status_polling_interval = 5
    m.launch_cmd = "launchy"
    m.interchange_launch_cmd = "ix-launchy"
    return m
