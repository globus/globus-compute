import contextlib
import getpass
import gzip
import io
import os
import pathlib
import platform
import typing as t
from unittest import mock

import pytest
from globus_compute_sdk.sdk.diagnostic import cat, do_diagnostic_base
from globus_compute_sdk.sdk.web_client import WebClient
from pytest_mock import MockFixture

MOCK_DIAG_BASE = "globus_compute_sdk.sdk.diagnostic"


@pytest.fixture
def change_test_dir(mock_gc_home, monkeypatch):
    monkeypatch.chdir(mock_gc_home)


@pytest.fixture
def mock_hardware_report(mocker):
    simple_hardware_report = [
        [
            os.cpu_count,
            platform.platform,
        ],
        [
            ["cpu_count", os.cpu_count()],
            ["platform", platform.platform()],
        ],
    ]
    cl = mocker.patch(f"{MOCK_DIAG_BASE}.hardware_commands_list")
    cl.return_value = simple_hardware_report[0]

    yield simple_hardware_report[1]


@pytest.fixture
def mock_general_reports(mocker, mock_gc_home):
    home_dir = str(mock_gc_home).strip()
    general_report = [
        [
            "whoami",
            "uname -a",
            cat(f"{home_dir}/*/*.yaml", wildcard=True, max_bytes=1024),
            cat(f"{home_dir}/*/*.log", wildcard=True, max_bytes=1024),
            # test_conn(get_web_service_url()),
        ],
        [
            ["whoami", getpass.getuser()],
            ["uname", platform.uname()[0]],
            ["*/*.yaml)", ""],
            ["*/*.log)", ""],
        ],
    ]
    cl = mocker.patch(f"{MOCK_DIAG_BASE}.general_commands_list")
    cl.return_value = general_report[0]

    yield general_report[1]


@pytest.fixture
def mock_gce_not_installed(mocker):
    dir_mock = mocker.patch(f"{MOCK_DIAG_BASE}.print_endpoint_install_dir")
    dir_mock.return_value = "globus-compute-endpoint is not installed"

    yield dir_mock


def test_cat_handles_non_binary_files_gracefully(fs):
    bfile = pathlib.Path("/some/binary/file_text")
    bfile.parent.mkdir(parents=True)
    bfile.write_text(str(os.urandom(64)))
    with contextlib.redirect_stdout(io.StringIO()) as f:
        cat(str(bfile))()
    assert len(f.getvalue()) > 64


def test_cat_handles_binary_files_gracefully(fs):
    bfile = pathlib.Path("/some/binary/file_binary")
    bfile.parent.mkdir(parents=True)
    bfile.write_bytes(os.urandom(64))
    with contextlib.redirect_stdout(io.StringIO()) as f:
        cat(str(bfile))()
    assert len(f.getvalue()) > 64


def test_cat_handles_missing_file_gracefully(fs):
    bfile = pathlib.Path("/some/file")
    bfile.parent.mkdir(parents=True)
    with contextlib.redirect_stdout(io.StringIO()) as f:
        cat(str(bfile))()
    expected_header = f"cat {bfile}"
    expected_header_hline = "=" * len(str(bfile))
    expected_footer_hline = "-" * len(str(bfile))
    assert expected_header in f.getvalue()
    assert expected_header_hline in f.getvalue()
    assert expected_footer_hline in f.getvalue()
    assert f"\nNo file named {bfile}" in f.getvalue()


@pytest.mark.parametrize("max_bytes", (-64, -32, -1, None, 0, 1, 32, 64))
def test_cat_honors_max_bytes(fs, max_bytes):
    raw_file_size = 100
    expected_sentinel = "abcde12345"
    bfile = pathlib.Path("/some/binary/file")
    bfile.parent.mkdir(parents=True)

    assert raw_file_size > (max_bytes or 0), "Verify test setup"
    fdata = expected_sentinel.encode() + os.urandom(raw_file_size)
    fdata = fdata.replace(b"\n", b"X")  # test splits on newline
    fdata = fdata.replace(b"\r", b"Y")  # test splits on newline
    bfile.write_bytes(fdata)

    fsize = len(bfile.read_bytes())
    if max_bytes:
        assert fsize > max_bytes, "Verify test setup: file size bigger than test's max"

    with contextlib.redirect_stdout(io.StringIO()) as f:
        if max_bytes is None:
            cat(str(bfile))()
        else:
            cat(str(bfile), max_bytes=max_bytes)()
    payload = "\n".join(f.getvalue().splitlines()[2:-2])  # remove header and footer
    payload = payload[3:]  # remove line header

    if max_bytes and max_bytes > 0:
        assert len(payload) <= max_bytes, fdata
        assert expected_sentinel not in payload, fdata
    else:
        # Some bytes may be merged as utf-8 random bytes may contain less chars
        # ie. len(b'abcde12345\xf9\xf0#\x86Z>:...') < len('abcde12345####Z>:...')
        assert len(payload) <= fsize, fdata
        assert expected_sentinel in payload, fdata


@pytest.mark.parametrize(
    "path",
    [
        "endpoint.log",
        "GlobusComputeEngine/interchange.log",
        # "GlobusComputeEngine/block-0/some-block-id/manager.log",
        # "GlobusComputeEngine/block-0/some-block-id/worker_0.log",
    ],
)
def test_cat_wildcard_finds_files_recursively(fs, path, randomstring):
    full_path = pathlib.Path("/some_ep_dir/" + path)
    full_path.parent.mkdir(parents=True, exist_ok=True)
    contents = randomstring()
    full_path.write_text(contents)

    with contextlib.redirect_stdout(io.StringIO()) as f:
        cat("/some_ep_dir/**/*.log", wildcard=True)()

    payload = str(f.getvalue())
    assert path in payload
    assert contents in payload


"""
Some following tests previously used fakefs, which apparently is no longer
compatible with the new structure with lower level system calls.  See:

https://stackoverflow.com/questions/75789398/how-to-use-pyfakefs-pytest-to-test-a-function-using-multiprocessing-queue-via  # noqa
"""


def test_diagnostic_simple(
    mocker: MockFixture,
    randomstring,
    mock_gc_home,
    mock_hardware_report,
    mock_general_reports,
    mock_gce_not_installed,
    change_test_dir,
    capsys,
):
    mock_sock = mock.MagicMock()
    mock_ssock = mock.MagicMock()

    # TODO fix SSL and get_version in test, or remove these
    mock_ssock.cipher.return_value = ("mocked_cipher", "mocked_version", None)
    mock_create_conn = mocker.patch("socket.create_connection")
    mock_create_conn.return_value.__enter__.return_value = mock_sock
    mock_create_ctx = mocker.patch("ssl.create_default_context")
    mock_create_ctx.return_value.wrap_socket.return_value.__enter__.return_value = (
        mock_ssock
    )
    mocker.patch.object(WebClient, "get_version")

    ep_name = "diagnostic_test"

    ep_dir_path = mock_gc_home / ep_name
    ep_dir_path.mkdir(exist_ok=False)

    conf_path = ep_dir_path / "config.yaml"
    log_path = ep_dir_path / "endpoint.log"
    conf_data = randomstring()
    log_data = randomstring()

    with open(conf_path, "w") as f:
        f.write(conf_data)

    with open(log_path, "w") as f:
        f.write(log_data)

    do_diagnostic_base(["-p", "-kb", "1"])
    captured = capsys.readouterr()

    assert captured.out.count("== Diagnostic") == 6
    for report in [mock_hardware_report, mock_general_reports]:
        for title, expected_output in report:
            assert title in captured.out
            assert str(expected_output) in captured.out

    # TODO fix
    # assert conf_data in captured.out
    # TODO fix
    # assert log_data in captured.out


@pytest.mark.parametrize("env", ["sandbox", "integration", "staging"])
def test_self_diagnostic_sdk_environment(
    mocker: MockFixture,
    mock_command_ensure,
    mock_cli_state,
    run_line: t.Callable,
    monkeypatch,
    mock_gc_home,
    env: str,
):
    mock_sock = mock.MagicMock()
    mock_ssock = mock.MagicMock()
    mock_ssock.cipher.return_value = ("mocked_cipher", "mocked_version", None)
    mock_create_conn = mocker.patch("socket.create_connection")
    mock_create_conn.return_value.__enter__.return_value = mock_sock
    mock_create_ctx = mocker.patch("ssl.create_default_context")
    mock_create_ctx.return_value.wrap_socket.return_value.__enter__.return_value = (
        mock_ssock
    )
    mocker.patch.object(WebClient, "get_version")

    mock_test_conn = mocker.patch(
        "globus_compute_endpoint.self_diagnostic.test_conn",
        return_value=lambda: "Success!",
    )
    mock_test_ssl_conn = mocker.patch(
        "globus_compute_endpoint.self_diagnostic.test_ssl_conn",
        return_value=lambda: "Success!",
    )

    monkeypatch.setenv("GLOBUS_SDK_ENVIRONMENT", env)
    run_line("self-diagnostic")

    assert f"compute.api.{env}.globuscs.info" in mock_test_conn.call_args_list[0][0]
    assert f"compute.amqps.{env}.globuscs.info" in mock_test_conn.call_args_list[1][0]
    assert f"compute.api.{env}.globuscs.info" in mock_test_ssl_conn.call_args_list[0][0]
    assert (
        f"compute.amqps.{env}.globuscs.info" in mock_test_ssl_conn.call_args_list[1][0]
    )


def test_self_diagnostic_gzip(
    mocker: MockFixture,
    mock_cli_state,
    mock_command_ensure,
    run_line: t.Callable,
    mock_gc_home,
):
    mock_sock = mock.MagicMock()
    mock_ssock = mock.MagicMock()
    mock_ssock.cipher.return_value = ("mocked_cipher", "mocked_version", None)
    mock_create_conn = mocker.patch("socket.create_connection")
    mock_create_conn.return_value.__enter__.return_value = mock_sock
    mock_create_ctx = mocker.patch("ssl.create_default_context")
    mock_create_ctx.return_value.wrap_socket.return_value.__enter__.return_value = (
        mock_ssock
    )
    mocker.patch.object(WebClient, "get_version")

    res = run_line("self-diagnostic --gzip")
    stdout = res.stdout_bytes.decode("utf-8")

    assert "Successfully created" in stdout
    assert "== Diagnostic" not in stdout

    for fname in os.listdir("."):
        if fname.endswith(".txt.gz"):
            break
    with gzip.open(fname, "rb") as f:
        contents = f.read().decode("utf-8")

    assert contents.count("== Diagnostic") >= 21


@pytest.mark.skip("disabled while being worked on")
@pytest.mark.parametrize("test_data", [(True, 1), (False, 0.5), (False, "")])
def test_self_diagnostic_log_size(
    mocker: MockFixture,
    mock_cli_state,
    mock_command_ensure,
    run_line: t.Callable,
    test_data: list[tuple[bool, int | float | str]],
    mock_gc_home,
):
    should_succeed, kb = test_data

    mock_sock = mock.MagicMock()
    mock_ssock = mock.MagicMock()
    mock_ssock.cipher.return_value = ("mocked_cipher", "mocked_version", None)
    mock_create_conn = mocker.patch("socket.create_connection")
    mock_create_conn.return_value.__enter__.return_value = mock_sock
    mock_create_ctx = mocker.patch("ssl.create_default_context")
    mock_create_ctx.return_value.wrap_socket.return_value.__enter__.return_value = (
        mock_ssock
    )
    mocker.patch.object(WebClient, "get_version")

    def run_cmd():
        res = run_line(f"self-diagnostic --log-kb {kb}")
        return res.stdout_bytes.decode("utf-8")

    if should_succeed:
        stdout = run_cmd()
        assert stdout.count("== Diagnostic") >= 21
    else:
        with pytest.raises(AssertionError):
            stdout = run_cmd()


@pytest.mark.skip("disabled while being worked on")
def test_self_diagnostic_log_size_limit(
    mocker: MockFixture,
    mock_cli_state,
    mock_command_ensure,
    run_line: t.Callable,
    ep_name,
    mock_gc_home,
):
    home_path = os.path.expanduser("~")
    ep_dir_path = f"{home_path}/.globus_compute/{ep_name}/"
    log_path = f"{ep_dir_path}/endpoint.log"

    mock_sock = mock.MagicMock()
    mock_ssock = mock.MagicMock()
    mock_ssock.cipher.return_value = ("mocked_cipher", "mocked_version", None)
    mock_create_conn = mocker.patch("socket.create_connection")
    mock_create_conn.return_value.__enter__.return_value = mock_sock
    mock_create_ctx = mocker.patch("ssl.create_default_context")
    mock_create_ctx.return_value.wrap_socket.return_value.__enter__.return_value = (
        mock_ssock
    )
    mocker.patch.object(WebClient, "get_version")

    def run_cmd():
        # Limit log file size to 1 KB
        res = run_line("self-diagnostic --log-kb 1")
        return res.stdout_bytes.decode("utf-8")

    f_size = 1024  # 1 KB
    with open(log_path, "w") as f:
        f.write("$" * f_size)
    stdout = run_cmd()

    f_size += 1  # Adding 1 extra byte
    with open(log_path, "w") as f:
        # Adding one extra byte
        f.write("$" * f_size)
    stdout_limited = run_cmd()

    assert len(stdout) == len(stdout_limited)
