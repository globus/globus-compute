import pytest
from funcx import FuncXClient

config = {
    # 'funcx_service_address': 'http://k8s-dev.funcx.org/api/v1',
    'funcx_service_address': 'http://127.0.0.1:5000/api/v1',   # For testing against local k8s
    'endpoint_uuid': 'e5d141b6-d87c-4aec-8e50-5cc2b37a207d',
}


@pytest.fixture(autouse=True, scope='session')
def load_funcx_session(request, pytestconfig):
    """Load funcX sdk client for the entire test suite,

    The special path `local` indicates that configuration will not come
    from a pytest managed configuration file; in that case, see
    load_dfk_local_module for module-level configuration management.
    """

    # config = pytestconfig.getoption('config')[0]


def pytest_addoption(parser):
    """Add funcx-specific command-line options to pytest.
    """
    parser.addoption(
        '--endpoint',
        action='store',
        metavar='endpoint',
        nargs=1,
        default=[config['endpoint_uuid']],
        help="Specify an active endpoint UUID"
    )

    parser.addoption(
        '--service-address',
        action='store',
        metavar='service-address',
        nargs=1,
        default=[config['funcx_service_address']],
        help="Specify a funcX service address"
    )


@pytest.fixture
def fxc(pytestconfig):
    fxc = FuncXClient(funcx_service_address=pytestconfig.getoption('--service-address')[0])
    return fxc


@pytest.fixture
def endpoint(pytestconfig):
    endpoint = pytestconfig.getoption('--endpoint')[0]
    return endpoint
