from __future__ import annotations

import shlex
from unittest import mock

import pytest
from click.testing import CliRunner

from funcx_endpoint.cli import app


@pytest.fixture
def funcx_dir_path(tmp_path):
    yield tmp_path / "funcx-dir"


@pytest.fixture(autouse=True)
def endpoint_obj(funcx_dir_path):
    with mock.patch("funcx_endpoint.cli.get_cli_endpoint") as m:
        mock_ep = mock.Mock()
        mock_ep.funcx_dir = funcx_dir_path
        mock_ep.funcx_config_file_name = "config.py"

        m.return_value = mock_ep
        yield mock_ep


@pytest.fixture
def make_endpoint_dir(endpoint_obj):
    def func(name):
        ep_dir = endpoint_obj.funcx_dir / name
        ep_dir.mkdir(parents=True, exist_ok=True)
        ep_config = ep_dir / endpoint_obj.funcx_config_file_name
        ep_config.write_text("config = 1")  # minimal setup to make loading work

    return func


@pytest.fixture
def cli_runner():
    return CliRunner(mix_stderr=False)


@pytest.fixture
def run_line(cli_runner):
    def func(argline, *, assert_exit_code: int | None = 0):
        args = shlex.split(argline) if isinstance(argline, str) else argline
        result = cli_runner.invoke(app, args)
        if assert_exit_code is not None:
            assert result.exit_code == assert_exit_code
        return result

    return func


def test_start_endpoint_no_such_ep(run_line, endpoint_obj):
    res = run_line("start foo")
    endpoint_obj.start_endpoint.assert_not_called()
    assert "Endpoint foo is not configured" in res.stdout


def test_start_endpoint_existing_ep(run_line, endpoint_obj, make_endpoint_dir):
    make_endpoint_dir("foo")
    run_line("start foo")
    endpoint_obj.start_endpoint.assert_called_once()


def test_stop_endpoint(run_line, endpoint_obj, make_endpoint_dir):
    run_line("stop foo")
    endpoint_obj.stop_endpoint.assert_called_once()


def test_restart_endpoint_does_start_and_stop(
    run_line, endpoint_obj, make_endpoint_dir
):
    make_endpoint_dir("foo")
    run_line("restart foo")

    endpoint_obj.stop_endpoint.assert_called_once()
    endpoint_obj.start_endpoint.assert_called_once()
