import pytest

from funcx import FuncXClient
from funcx.sdk.executor import FuncXExecutor

config = {
    "funcx_service_address": "https://api2.funcx.org/v2",
    "endpoint_uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "results_ws_uri": "wss://api2.funcx.org/ws/v2/",
}


@pytest.fixture(autouse=True, scope="session")
def load_funcx_session(request, pytestconfig):
    """Load funcX sdk client for the entire test suite,

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
        help="Specify a funcX service address",
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
def fxc_args(pytestconfig):
    fxc_args = {
        "funcx_service_address": pytestconfig.getoption("--service-address")[0],
        "results_ws_uri": pytestconfig.getoption("--ws-uri")[0],
    }
    return fxc_args


@pytest.fixture
def fxc(fxc_args):
    fxc = FuncXClient(**fxc_args)
    return fxc


@pytest.fixture
def async_fxc(fxc_args):
    fxc = FuncXClient(**fxc_args, asynchronous=True)
    return fxc


@pytest.fixture
def fx(fxc):
    fx = FuncXExecutor(fxc)
    return fx


@pytest.fixture
def batch_fx(fxc):
    fx = FuncXExecutor(fxc, batch_enabled=True)
    return fx


@pytest.fixture
def endpoint(pytestconfig):
    endpoint = pytestconfig.getoption("--endpoint")[0]
    return endpoint
