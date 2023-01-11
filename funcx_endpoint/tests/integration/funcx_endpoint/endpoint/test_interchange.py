import logging
import pathlib
from importlib.machinery import SourceFileLoader

import pytest

from funcx_endpoint.endpoint.config import Config
from funcx_endpoint.endpoint.endpoint import Endpoint
from funcx_endpoint.endpoint.interchange import EndpointInterchange

logger = logging.getLogger("mock_funcx")


@pytest.fixture
def funcx_dir(tmp_path):
    fxdir = tmp_path / pathlib.Path("funcx")
    fxdir.mkdir()
    yield fxdir


def test_endpoint_id(mocker, funcx_dir):
    mock_client = mocker.patch("funcx_endpoint.endpoint.interchange.FuncXClient")
    mock_client.return_value = None

    manager = Endpoint(funcx_dir=str(funcx_dir))
    config_dir = funcx_dir / "mock_endpoint"

    manager.configure_endpoint(config_dir, None)
    endpoint_config = SourceFileLoader(
        "config", str(funcx_dir / "mock_endpoint" / "config.py")
    ).load_module()

    for executor in endpoint_config.config.executors:
        executor.passthrough = False

    ic = EndpointInterchange(
        endpoint_config.config,
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        endpoint_id="mock_endpoint_id",
    )

    for executor in ic.executors.values():
        assert executor.endpoint_id == "mock_endpoint_id"


def test_start_requires_pre_registered(mocker, funcx_dir):
    with pytest.raises(TypeError):
        EndpointInterchange(
            config=Config(),
            funcx_client=mocker.Mock(),
            reg_info=None,
            endpoint_id="mock_endpoint_id",
        )
