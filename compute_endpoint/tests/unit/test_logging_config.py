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
from globus_compute_sdk.sdk.compute_dir import COMPUTE_EP_DIR_ENV, ensure_compute_dir
from pytest_mock import MockFixture

_MOCK_BASE = "globus_compute_endpoint.logging_config."
_MOCK_GCC_DIR = "/tmp/.gc"


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


@pytest.mark.parametrize("ep_dir_env", ("/path/to/ep_dir", None))
def test_ensure_paths_no_name_provided(fs, mock_ensure_compute, ep_dir_env):
    env_dict = {"random_env": "value"}
    if ep_dir_env:
        env_dict[COMPUTE_EP_DIR_ENV] = ep_dir_env

    with mock.patch.dict(os.environ, env_dict, clear=True):
        if ep_dir_env:
            # If the environment variable is set, ep_name is not required
            assert f"{ep_dir_env}/endpoint.log" == str(ensure_paths("").resolve())
        else:
            with pytest.raises(ValueError) as pyt_exc:
                ensure_paths("")
            assert f"Endpoint name must be provided" in str(pyt_exc.value)


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
            with pytest.raises(ValueError) as pyt_exc:
                ensure_paths("ep1")
            assert exc_msg in str(pyt_exc.value)
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

    a_path.parent.mkdir(parents=True)
    if is_file:
        a_path.touch(exist_ok=True)
    else:
        a_path.mkdir()

    with mock.patch.dict(
        os.environ, {COMPUTE_EP_DIR_ENV: str(a_path.resolve())}, clear=True
    ):
        if exc_msg:
            with pytest.raises(ValueError) as pyt_exc:
                ensure_paths("ep1")
            assert exc_msg in str(pyt_exc.value)
        else:
            # Should create the dir structure
            result_path = ensure_paths("ep1")
            assert result_path == a_path / "endpoint.log"
            assert result_path.parent.exists()


@pytest.mark.parametrize(
    ("env_dict", "paths", "exp_ep", "exp_log"),
    (
        (
            {COMPUTE_EP_DIR_ENV: "$HOME/ep1"},
            {"endpoint_dir": "~/ep2"},
            "/home/foo/ep2",
            "/home/foo/ep2/endpoint.log",
        ),
        (
            {COMPUTE_EP_DIR_ENV: "$HOME/ep1"},
            {"endpoint_dir": "$HOME/ep2"},
            "/home/foo/ep2",
            "/home/foo/ep2/endpoint.log",
        ),
        (
            {LOG_PATH_ENV: "$HOME/ep1/b.log"},
            {"endpoint_log": "~/$USER/a.log"},
            "/home/foo/.globus_compute/ep1",
            "/home/foo/bar/a.log",
        ),
        (
            {},
            {},
            "/home/foo/.globus_compute/ep1",
            "/home/foo/.globus_compute/ep1/endpoint.log",
        ),
    ),
)
def test_ensure_paths_expand_set(fs, env_dict, paths, exp_ep, exp_log):
    ep_name = "ep1"
    home = "/home/foo"
    env = {"HOME": home, "USER": "bar"}
    env.update(env_dict)

    with mock.patch.dict(os.environ, env, clear=True):
        path_result = ensure_paths(ep_name, paths)
        assert str(path_result) == exp_log
        assert os.environ[COMPUTE_EP_DIR_ENV] == exp_ep
        assert os.environ[LOG_PATH_ENV] == exp_log


@pytest.mark.parametrize(
    "env",
    (
        {COMPUTE_EP_DIR_ENV: "/a/env_ep_dir", LOG_PATH_ENV: "/x/env_file.log"},
        {COMPUTE_EP_DIR_ENV: "/a/env_ep_dir"},
        {LOG_PATH_ENV: "/x/env_file.log"},
        {},
    ),
)
@pytest.mark.parametrize(
    "paths",
    (
        {"endpoint_dir": "/b/config_ep_dir", "endpoint_log": "/y/log_file.log"},
        {"endpoint_dir": "/b/../a/config_ep_dir"},
        {"endpoint_dir": "/b/config_ep_dir"},
        {"endpoint_log": "/y/../x/log_file.log"},
        {},
    ),
)
@pytest.mark.parametrize("ep_name", ("some_ep_name", None))
def test_ensure_paths_config_overrides_resolve_env(fs, env, paths, ep_name):
    if not (ep_name or paths.get("endpoint_dir") or env.get(COMPUTE_EP_DIR_ENV)):
        # This (nothing given) errors, tested in test_ensure_paths_no_named_provided
        return

    env.update({"HOME": "/home/foo", "USER": "bar"})
    default_dir = f"{env['HOME']}/.globus_compute/{ep_name}"
    exp_ep = paths.get("endpoint_dir", env.get(COMPUTE_EP_DIR_ENV, default_dir))
    exp_log = paths.get("endpoint_log", env.get(LOG_PATH_ENV, f"{exp_ep}/endpoint.log"))

    with mock.patch.dict(os.environ, env):
        log_result = str(ensure_paths(ep_name, paths).resolve())
        assert log_result == str(pathlib.Path(exp_log).resolve())
        assert os.environ.get(COMPUTE_EP_DIR_ENV) == str(pathlib.Path(exp_ep).resolve())


def test_ensure_paths_default_uses_name_and_compute_dir(fs, mock_ensure_compute):
    ep_name = "ep1"
    expected_log_path = pathlib.Path(_MOCK_GCC_DIR) / ep_name / "endpoint.log"
    with mock.patch.dict(os.environ, {"HOME": "/home/foo"}, clear=True):
        log_result_path = ensure_paths(ep_name)
        # Have to use str(..resolve()) as (expected_log_path == log_result_path) == False
        assert str(expected_log_path) == str(log_result_path)
        assert str(expected_log_path.parent) == os.environ[COMPUTE_EP_DIR_ENV]
