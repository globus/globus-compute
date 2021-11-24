import os

import pytest
from typer.testing import CliRunner

from funcx_endpoint.endpoint.endpoint import app

runner = CliRunner()


config_string = """
from funcx_endpoint.endpoint.utils.config import Config
from parsl.providers import LocalProvider

config = Config(
    scaling_enabled=True,
    provider=LocalProvider(
        init_blocks=1,
        min_blocks=1,
        max_blocks=1,
    ),
    funcx_service_address='https://api.funcx.org/v1'
)"""


class TestEndpoint:
    @pytest.fixture(autouse=True)
    def test_setup_teardown(self, mocker):
        mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        yield

    def test_non_configured_endpoint(self, mocker):
        result = runner.invoke(app, ["start", "newendpoint"])
        assert "newendpoint" in result.stdout
        assert "not configured" in result.stdout

    def test_using_outofdate_config(self, mocker):
        mock_loader = mocker.patch("funcx_endpoint.endpoint.endpoint.os.path.join")
        mock_loader.return_value = "./config.py"
        config_file = open("./config.py", "w")
        config_file.write(config_string)
        config_file.close()
        result = runner.invoke(app, ["start", "newendpoint"])
        os.remove("./config.py")
        assert isinstance(result.exception, TypeError)
