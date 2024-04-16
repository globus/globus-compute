import uuid

import pytest
from globus_compute_sdk import Client

config = {
    "endpoint_uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "environment": "production",
    "local_compute_services": False,
}


def pytest_addoption(parser):
    """Add funcx-specific command-line options to pytest."""
    parser.addoption(
        "--endpoint",
        type=uuid.UUID,
        default=[config["endpoint_uuid"]],
        help="Specify an active endpoint UUID.",
    )

    parser.addoption(
        "--environment",
        default=[config["environment"]],
        help=(
            "Specify a Globus environment to connect to. If local-compute-services is"
            " not set, this also specifies what Compute environment to connect to."
        ),
    )

    parser.addoption(
        "--local-compute-services",
        action="store_true",
        default=[config["local_compute_services"]],
        help=(
            "If set, point the SDK to a locally running Compute cluster."
            " Overrides --environment."
        ),
    )


@pytest.fixture
def compute_client_args(pytestconfig):
    gcc_args = {
        "environment": pytestconfig.getoption("--environment")[0],
        "local_compute_services": pytestconfig.getoption("--local-compute-services")[0],
    }
    return gcc_args


@pytest.fixture
def compute_client(compute_client_args):
    gcc = Client(**compute_client_args)
    return gcc
