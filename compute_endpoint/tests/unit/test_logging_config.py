import logging.config
import os
import pathlib
import platform
import typing as t
from unittest import mock

import pytest
from globus_compute_endpoint.logging_config import (
    LOG_PATH_ENV,
    _get_file_dict_config,
    ensure_paths,
    setup_logging,
)
from globus_compute_sdk.sdk.compute_dir import COMPUTE_EP_DIR_ENV
from pytest_mock import MockFixture

_MOCK_BASE = "globus_compute_endpoint.logging_config."
_MOCK_GCC_DIR = "/tmp/.gc"  # Use this only with fs


@pytest.fixture
def anon_pipe():
    read, write = os.pipe()
    yield read, write
    os.close(write)
    os.close(read)


@pytest.fixture
def mock_ensure_compute(fs):
    with mock.patch(
        f"{_MOCK_BASE}ensure_compute_dir", return_value=pathlib.Path(_MOCK_GCC_DIR)
    ) as m:
        yield m


def test_verify_setup_logging_test_hookups_metatest(mocker):
    mock_impl_file = mocker.patch(f"{_MOCK_BASE}_get_file_dict_config")
    mock_impl_stream = mocker.patch(f"{_MOCK_BASE}_get_stream_dict_config")
    mock_log_config = mocker.patch(f"{_MOCK_BASE}logging.config.dictConfig")

    k = {"console_enabled": False, "debug": False, "no_color": True}
    setup_logging(**k)
    assert mock_impl_stream.called, "Verify test module assumption"
    assert not mock_impl_file.called, "Verify test module assumption"
    assert mock_log_config.called, "Verify test module assumption"

    mock_impl_stream.reset_mock()
    mock_log_config.reset_mock()

    k["logfile"] = "/some/path"
    setup_logging(**k)
    assert not mock_impl_stream.called, "Verify test module assumption"
    assert mock_impl_file.called, "Verify test module assumption"
    assert mock_log_config.called, "Verify test module assumption"


def test_file_config_rotates_log(fs):
    logp = pathlib.Path("/some/path/some/file.log")
    conf = _get_file_dict_config(logp, False, False, True)
    file_handler = conf["handlers"]["logfile"]

    assert "Rotating" in file_handler["class"]
    assert "maxBytes" in file_handler, "Without maxBytes, default setup won't rotate"


def test_file_config_rotates_at_reasonable_size(fs):
    logp = pathlib.Path("/some/path/some/file.log")
    conf = _get_file_dict_config(logp, False, False, True)
    file_handler = conf["handlers"]["logfile"]

    assert file_handler["maxBytes"] > 1024, "Expected *some* file-rotation threshold"
    assert file_handler["maxBytes"] <= 2**30, "A gigabyte is perhaps large enough"


def test_file_config_does_not_rotate_unrotatable_sc30480(anon_pipe):
    read_h, write_h = anon_pipe
    if platform.system() == "Darwin":
        # macOS doesn't have /proc, /dev is equivalent for this test
        logp = pathlib.Path(f"/dev/fd/{write_h}")
    else:
        # Should be "linux", "Windows" should have other problems
        logp = pathlib.Path(f"/proc/self/fd/{write_h}")
    conf = _get_file_dict_config(logp, False, False, True)

    file_handler = conf["handlers"]["logfile"]
    assert "Rotating" not in file_handler["class"], "Expected a non-rotating handler"


@pytest.mark.parametrize("logfile", ("/path/to/logfile", None))
def test_include_correct_loggers(logfile: t.Optional[str], mocker: MockFixture, fs):
    mock_dictConfig = mocker.patch.object(logging.config, "dictConfig")

    setup_logging(logfile=logfile)

    expected = {
        "globus_compute_endpoint",
        "globus_compute_sdk",
        "parsl",
    }
    loggers = mock_dictConfig.call_args[0][0]["loggers"]
    assert set(loggers) == expected, "Time to update this test?"


@pytest.mark.parametrize("ep_name", ("", None))
def test_ensure_paths_no_name_provided(fs, mock_ensure_compute, ep_name):
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError) as pyt_exc:
            ensure_paths(ep_name)
    assert f"Endpoint name must be provided" in str(pyt_exc.value)


def test_ensure_path_log_path_not_dir(fs):
    a_path = pathlib.Path("/a/b/c/d")
    a_path.mkdir(parents=True)
    with mock.patch.dict(os.environ, {LOG_PATH_ENV: str(a_path)}, clear=True):
        with pytest.raises(ValueError) as pyt_exc:
            ensure_paths("ep1")
        exc_msg = str(pyt_exc.value)
        assert LOG_PATH_ENV in exc_msg, "Expect relevant env var referenced"
        assert "can not be a directory" in exc_msg, "Expect cause"
        assert str(a_path) in exc_msg, "Expect problem value in msg"


def test_ensure_path_ep_dir_not_dir(fs):
    a_path = pathlib.Path("/a/b/c/d")
    a_path.parent.mkdir(parents=True)
    a_path.touch()
    with mock.patch.dict(os.environ, {COMPUTE_EP_DIR_ENV: str(a_path)}, clear=True):
        with pytest.raises(ValueError) as pyt_exc:
            ensure_paths("ep1")
        exc_msg = str(pyt_exc.value)
        assert COMPUTE_EP_DIR_ENV in exc_msg, "Expect relevant env var referenced"
        assert "must be a directory" in exc_msg, "Expect cause"
        assert str(a_path) in exc_msg, "Expect problem value in msg"


@pytest.mark.parametrize(
    "env",
    (
        {COMPUTE_EP_DIR_ENV: "/a/env_ep_dir", LOG_PATH_ENV: "/x/env_file.log"},
        {COMPUTE_EP_DIR_ENV: "~/env_ep_dir", LOG_PATH_ENV: "/tmp/$USER/a/env_file.log"},
        {COMPUTE_EP_DIR_ENV: "/tmp/$USER/env_ep_dir", LOG_PATH_ENV: "~/a/env_file.log"},
        {COMPUTE_EP_DIR_ENV: "/a/env_ep_dir"},
        {LOG_PATH_ENV: "/x/env_file.log"},
        {},
    ),
)
@pytest.mark.parametrize(
    "paths",
    (
        {"endpoint_dir": "/b/config_ep_dir", "endpoint_log": "/y/log_file.log"},
        {"endpoint_dir": "$HOME/b/config_ep_dir"},  # Tests expands for dir
        {"endpoint_dir": "/b/../a/config_ep_dir"},  # Tests .resolve() for dir
        {"endpoint_log": "/y/../x/log_file.log"},  # Tests .resolve() for log
        {"endpoint_log": "~/$USER/y/../x/log_file.log"},  # Tests expands and resolve
        {},
    ),
)
@pytest.mark.parametrize("ep_name", ("some_ep_name", None))
def test_ensure_paths_expand_resolve_config_overrides_env(fs, env, paths, ep_name):
    """
    This tests
        1) paths.* overrides env variables
        2) Relative paths such as /a/../b/c in log_path or ep_dir gets resolved
        3) Env vars such as $HOME gets expanded, as are ~/a/b/c format
    """
    if not (ep_name or paths.get("endpoint_dir") or env.get(COMPUTE_EP_DIR_ENV)):
        # This (nothing given) errors, tested in test_ensure_paths_no_name_provided
        return

    env.update({"HOME": "/home/foo", "USER": "bar"})
    default_dir = f"{env['HOME']}/.globus_compute/{ep_name}"
    exp_ep = paths.get("endpoint_dir", env.get(COMPUTE_EP_DIR_ENV, default_dir))
    exp_log = paths.get("endpoint_log", env.get(LOG_PATH_ENV, f"{exp_ep}/endpoint.log"))

    with mock.patch.dict(os.environ, env):
        exp_ep = pathlib.Path(os.path.expandvars(exp_ep)).expanduser().resolve()
        exp_log = pathlib.Path(os.path.expandvars(exp_log)).expanduser().resolve()
        log_result = ensure_paths(ep_name, paths)
        assert os.environ[COMPUTE_EP_DIR_ENV] == str(exp_ep), "Expect env always set"
        assert os.environ[LOG_PATH_ENV] == str(exp_log), "Expect env always set"
        assert str(log_result) == str(exp_log)

        assert exp_ep.stat().st_mode & 0o777 == 0o700
        assert exp_log.parent.stat().st_mode & 0o777 == 0o700


def test_ensure_paths_default_uses_name_and_compute_dir(fs, mock_ensure_compute):
    ep_name = "ep1"
    expected_log_path = pathlib.Path(_MOCK_GCC_DIR) / ep_name / "endpoint.log"
    with mock.patch.dict(os.environ, {"HOME": "/home/foo"}, clear=True):
        log_result_path = ensure_paths(ep_name)
        assert str(expected_log_path) == str(log_result_path)
        assert str(expected_log_path.parent) == os.environ[COMPUTE_EP_DIR_ENV]
