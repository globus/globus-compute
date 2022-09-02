import pytest
from click.testing import CliRunner

from funcx_endpoint.cli import app

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
    funcx_service_address='https://api.funcx.org/v2'
)"""


@pytest.fixture(autouse=True)
def patch_funcx_client(mocker):
    mocker.patch("funcx_endpoint.endpoint.endpoint.FuncXClient")


def test_non_configured_endpoint(mocker):
    result = runner.invoke(app, ["start", "newendpoint"])
    assert "newendpoint" in result.stdout
    assert "not configured" in result.stdout
