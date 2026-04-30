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


@pytest.fixture
def anon_pipe():
    read, write = os.pipe()
    yield read, write
    os.close(write)
    os.close(read)


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


def test_ensure_paths_no_default_error():
    with mock.patch.dict(os.environ, {"random_env": "value"}):
        with pytest.raises(ValueError) as actual_exc_msg:
            ensure_paths("")
        assert f"Endpoint name must be provided" in str(actual_exc_msg)


@pytest.mark.parametrize(
    ("is_dir", "exc_msg"),
    (
        [True, "can not be a directory"],
        [False, None],
        [None, None],
    ),
)
def test_ensure_path_log_path_not_dir(fs, is_dir, exc_msg):
    a_path = pathlib.Path("/a/b/c/d")
    if is_dir is None:
        # None to signify file doesn't exist
        a_path.parent.mkdir(parents=True)
    elif is_dir:
        a_path.mkdir(parents=True)
    else:
        a_path.parent.mkdir(parents=True)
        a_path.touch()

    with mock.patch.dict(os.environ, {LOG_PATH_ENV: str(a_path.resolve())}):
        if exc_msg:
            with pytest.raises(ValueError) as actual_exc_msg:
                ensure_paths("ep1")
            assert exc_msg in str(actual_exc_msg)
        else:
            # Should create the dir structure
            result_path = ensure_paths("ep1")
            assert result_path == a_path
            assert result_path.parent.exists()


@pytest.mark.parametrize(
    ("is_file", "exc_msg"),
    (
        [True, "can not be an existing file"],
        [False, None],
        [None, None],
    ),
)
def test_ensure_paths_ep_dir_not_file(fs, is_file, exc_msg):
    a_path = pathlib.Path("/a/b/c")
    if is_file is None:
        a_path.parent.mkdir(parents=True)
    elif is_file:
        a_path.parent.mkdir(parents=True)
        a_path.touch(exist_ok=True)
    else:
        a_path.mkdir(parents=True)

    with mock.patch.dict(os.environ, {COMPUTE_EP_DIR_ENV: str(a_path.resolve())}):
        if exc_msg:
            with pytest.raises(ValueError) as actual_exc_msg:
                ensure_paths("ep1")
            assert exc_msg in str(actual_exc_msg)
        else:
            # Should create the dir structure
            result_path = ensure_paths("ep1")
            assert result_path == a_path / "endpoint.log"
            assert result_path.parent.exists()


@pytest.mark.parametrize(
    ("ep_env", "log_env", "expected_path_str"),
    (
        (None, "/a/b/c.txt", "/a/b/c.txt"),
        ("/a/b/c", None, "/a/b/c/endpoint.log"),
        ("/a/b/c", "/d/e/f.txt", "/d/e/f.txt"),
    ),
)
def test_ensure_paths_env_order(fs, ep_env, log_env, expected_path_str):
    expected_path = pathlib.Path(expected_path_str)

    with mock.patch.dict(
        os.environ,
        {
            "HOME": "/home/foo",
            COMPUTE_EP_DIR_ENV: ep_env or "",
            LOG_PATH_ENV: log_env or "",
        },
    ):
        mock.patch(f"{_MOCK_BASE}ensure_compute_dir", return_value="/tmp/.gc")
        paths_arg = {}
        if ep_env:
            paths_arg[COMPUTE_EP_DIR_ENV] = ep_env
        if log_env:
            paths_arg[LOG_PATH_ENV] = log_env

        result_path = ensure_paths("xyz", paths_arg)
        assert result_path == expected_path
        assert expected_path.parent.exists()
        assert expected_path.parent.is_dir()


@pytest.mark.parametrize(
    ("ep_name", "ep_env", "log_env", "exp_ep", "exp_log"),
    (
        (
            "ep1",
            "$HOME/ep2",
            None,
            "/home/foo/ep2",
            "/home/foo/ep2/endpoint.log",
        ),
        (
            "ep1",
            None,
            "$HOME/a.b",
            "/home/foo/.globus_compute/ep1",
            "/home/foo/a.b",
        ),
    ),
)
@pytest.mark.skip("TODO FIX BEFORE PR SUBMISSION")
def test_ensure_paths_set_expand_envs(fs, ep_name, ep_env, log_env, exp_ep, exp_log):
    paths = {}
    env_dict = {"HOME": "/home/foo", "USER": "bar"}
    if ep_env:
        paths["endpoint_dir"] = ep_env
        env_dict[COMPUTE_EP_DIR_ENV] = ep_env
    if log_env:
        paths["endpoint_log"] = log_env
        env_dict[LOG_PATH_ENV] = log_env

    with mock.patch.dict(os.environ, env_dict):
        path_result = ensure_paths(ep_name, paths)
        assert str(path_result.resolve()) == exp_log
        if ep_env:
            assert os.environ[COMPUTE_EP_DIR_ENV] == exp_ep
        assert os.environ[LOG_PATH_ENV] == exp_log
