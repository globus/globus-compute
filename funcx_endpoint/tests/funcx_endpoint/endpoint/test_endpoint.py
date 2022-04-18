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
    result = runner.invoke(app, ["start", "--name", "newendpoint"])
    assert "newendpoint" in result.stdout
    assert "not configured" in result.stdout


def test_using_outofdate_config(mocker, tmp_path):
    config_file = tmp_path / "config.py"
    config_file.write_text(config_string)
    mock_loader = mocker.patch("funcx_endpoint.endpoint.endpoint.os.path.join")
    mock_loader.return_value = str(config_file)
    result = runner.invoke(app, ["start", "--name", "newendpoint"])
    assert isinstance(result.exception, TypeError)
