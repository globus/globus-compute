from __future__ import annotations

import os
import shlex
from unittest import mock

import pytest
from click.testing import CliRunner

from funcx_endpoint.cli import app


@pytest.fixture(autouse=True)
def endpoint_obj(tmp_path):
    funcx_dir = tmp_path / "funcx-dir"
    with mock.patch("funcx_endpoint.cli.get_cli_endpoint") as m:
        mock_ep = mock.Mock()
        mock_ep.funcx_dir = str(funcx_dir)
        mock_ep.funcx_config_file_name = "config.py"

        m.return_value = mock_ep
        yield mock_ep


@pytest.fixture
def make_endpoint_dir(endpoint_obj):
    def func(name):
        ep_dir = os.path.join(endpoint_obj.funcx_dir, name)
        ep_config = os.path.join(ep_dir, "config.py")
        os.makedirs(ep_dir, exist_ok=True)
        # touch the config file, so loading will work
        with open(ep_config, "w"):
            pass

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


def test_restart_endpoint_does_start_and_stop(
    run_line, endpoint_obj, make_endpoint_dir
):
    make_endpoint_dir("foo")
    run_line("restart foo")

    endpoint_obj.stop_endpoint.assert_called_once()
    endpoint_obj.start_endpoint.assert_called_once()
