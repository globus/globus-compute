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
    ensure_log_path,
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


@pytest.mark.parametrize()
@pytest.mark.parametrize(
    ("log_path", "envs", "is_dir", "exists", "expected_path", "exc_msg"),
    (
        ["/a/a_dir", {}, True, True, None, f"{LOG_PATH_ENV} can not be a directory"],
        ["/b/$XYZ/file.log", {"XYZ": "abc"}, False, False, "/b/abc/file.log", None],
        ["/d/file.log", {}, False, False, "/d/file.log", None],
        [None, {}, False, False, None, None],
    ),
)
def test_ensure_log_path(
    fs, mock_ep_dir, log_path, envs, is_dir, exists, expected_path, exc_msg
):
    _, ep_dir = mock_ep_dir
    if log_path:
        envs[LOG_PATH_ENV] = log_path
        p = pathlib.Path(log_path)
        if is_dir:
            p.mkdir(parents=True)
        elif exists:
            # create file but non-writable if file
            p.parent.mkdir(parents=True)
            p.touch(mode=0o400)
    envs[COMPUTE_EP_DIR_ENV] = str(ep_dir.resolve())

    with mock.patch.dict(os.environ, envs):
        if exc_msg is not None:
            with pytest.raises(Exception) as actual_exc_msg:
                ensure_log_path()
            assert exc_msg in str(actual_exc_msg)
        else:
            result_path: pathlib.Path = ensure_log_path()
            if log_path is None:
                # Default - use ep_dir
                assert result_path == ep_dir / "endpoint.log"
            else:
                # Custom log_path
                assert str(result_path.resolve()) == expected_path
                result_path.write_text("I can write to log file")
