import pytest
from globus_compute_sdk import Client

config = {
    "funcx_service_address": "https://compute.api.globus.org/v2",
    "endpoint_uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "results_ws_uri": "wss://compute.api.globus.org/ws/v2/",
}


@pytest.fixture(autouse=True, scope="session")
def load_funcx_session(request, pytestconfig):
    """Load Globus Compute sdk client for the entire test suite,

    The special path `local` indicates that configuration will not come
    from a pytest managed configuration file; in that case, see
    load_dfk_local_module for module-level configuration management.
    """

    # config = pytestconfig.getoption('config')[0]


def pytest_addoption(parser):
    """Add funcx-specific command-line options to pytest."""
    parser.addoption(
        "--endpoint",
        action="store",
        metavar="endpoint",
        nargs=1,
        default=[config["endpoint_uuid"]],
        help="Specify an active endpoint UUID",
    )

    parser.addoption(
        "--service-address",
        action="store",
        metavar="service-address",
        nargs=1,
        default=[config["funcx_service_address"]],
        help="Specify a Globus Compute service address",
    )

    parser.addoption(
        "--ws-uri",
        action="store",
        metavar="ws-uri",
        nargs=1,
        default=[config["results_ws_uri"]],
        help="WebSocket URI to get task results",
    )


@pytest.fixture
def compute_client_args(pytestconfig):
    gcc_args = {
        "funcx_service_address": pytestconfig.getoption("--service-address")[0],
        "results_ws_uri": pytestconfig.getoption("--ws-uri")[0],
    }
    return gcc_args


@pytest.fixture
def compute_client(compute_client_args):
    gcc = Client(**compute_client_args)
    return gcc


@pytest.fixture
def login_manager(mocker):
    mock_login_manager = mocker.Mock()
    mock_login_manager.get_web_client = mocker.Mock
    return mock_login_manager
