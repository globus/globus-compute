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
def mock_cli_state(funcx_dir_path):
    with mock.patch("funcx_endpoint.cli.get_cli_endpoint") as mock_cli:
        mock_ep = mock.Mock()
        mock_cli.return_value = mock_ep
        with mock.patch("funcx_endpoint.cli.CommandState.ensure") as m_state:
            mock_state = mock.Mock()
            mock_state.endpoint_config_dir = funcx_dir_path
            m_state.return_value = mock_state

            yield mock_ep, mock_state


@pytest.fixture
def make_endpoint_dir(mock_cli_state):
    mock_cli, mock_state = mock_cli_state

    def func(name):
        ep_dir = mock_state.endpoint_config_dir / name
        ep_dir.mkdir(parents=True, exist_ok=True)
        ep_config = ep_dir / "config.py"
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
            assert result.exit_code == assert_exit_code, (result.stdout, result.stderr)
        return result

    return func


def test_start_ep_corrupt(run_line, mock_cli_state, make_endpoint_dir):
    make_endpoint_dir("foo")
    mock_ep, mock_state = mock_cli_state
    conf = mock_state.endpoint_config_dir / "foo" / "config.py"
    conf.unlink()
    res = run_line("start foo", assert_exit_code=1)
    assert "corrupted?" in res.stderr


def test_start_endpoint_no_such_ep(run_line, mock_cli_state):
    res = run_line("start foo", assert_exit_code=1)
    mock_ep, _ = mock_cli_state
    mock_ep.start_endpoint.assert_not_called()
    assert "Endpoint 'foo' is not configured" in res.stderr


def test_start_endpoint_existing_ep(run_line, mock_cli_state, make_endpoint_dir):
    make_endpoint_dir("foo")
    run_line("start foo")
    mock_ep, _ = mock_cli_state
    mock_ep.start_endpoint.assert_called_once()


def test_stop_endpoint(run_line, mock_cli_state, make_endpoint_dir):
    run_line("stop foo")
    mock_ep, _ = mock_cli_state
    mock_ep.stop_endpoint.assert_called_once()


def test_restart_endpoint_does_start_and_stop(
    run_line, mock_cli_state, make_endpoint_dir
):
    make_endpoint_dir("foo")
    run_line("restart foo")

    mock_ep, _ = mock_cli_state
    mock_ep.stop_endpoint.assert_called_once()
    mock_ep.start_endpoint.assert_called_once()


def test_start_ep_incorrect(run_line, mock_cli_state, make_endpoint_dir):
    make_endpoint_dir("foo")
    mock_ep, mock_state = mock_cli_state
    conf = mock_state.endpoint_config_dir / "foo" / "config.py"

    conf.write_text("asa asd df = 5")  # fail the import
    with mock.patch("funcx_endpoint.cli.log") as mock_log:
        res = run_line("start foo", assert_exit_code=1)
        assert "might be out of date" in mock_log.exception.call_args[0][0]
    assert isinstance(res.exception, SyntaxError)

    # `coverage` demands a valid syntax file.  FBOW, then, the ordering and
    # commingling of these two tests is intentional.  Bit of a meta problem ...
    conf.unlink()
    conf.write_text("asdf = 5")  # syntactically correct
    res = run_line("start foo", assert_exit_code=1)
    assert "modified incorrectly?" in res.stderr
