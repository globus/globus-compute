import contextlib
import getpass
import gzip
import io
import os
import pathlib
import platform
from unittest import mock

import pytest
from globus_compute_sdk.sdk.diagnostic import cat, do_diagnostic_base
from globus_compute_sdk.sdk.web_client import WebClient
from pytest_mock import MockFixture

MOCK_DIAG_BASE = "globus_compute_sdk.sdk.diagnostic"


@pytest.fixture
def change_test_dir(mock_gc_home, monkeypatch):
    monkeypatch.chdir(mock_gc_home)
    yield mock_gc_home


"""
Change this definition if the default changes in terms of print/zipped_file
behavior.  ie. an empty list [] for creating zip files if that is the default,
["-p"] if the default is creating a zip file and printing to console is needed.
"""
DIAG_PRINT_ARGS = ["-p"]
DIAG_ZIP_ARGS = []


@pytest.fixture
def mock_hardware_report(mocker):
    simple_hardware_report = [
        [
            os.cpu_count,
            platform.platform,
        ],
        [
            ["cpu_count", str(os.cpu_count())],
            ["platform", platform.platform()],
        ],
    ]
    cl = mocker.patch(f"{MOCK_DIAG_BASE}.hardware_commands_list")
    cl.return_value = simple_hardware_report[0]

    yield simple_hardware_report[1]


@pytest.fixture
def mock_connection_tests(mocker):
    cl = mocker.patch(f"{MOCK_DIAG_BASE}.connection_tests")
    cl.return_value = []

    yield []


@pytest.fixture
def mock_general_reports(mocker, mock_gc_home, mock_endpoint_config_dir_data):
    home_dir = str(mock_gc_home).strip()
    general_report = [
        [
            "whoami",
            "uname -a",
            cat(
                [
                    f"{home_dir}/*/*.yaml",
                    f"{home_dir}/*/*.log",
                ],
                wildcard=True,
                max_bytes=1024,
            ),
        ],
        [
            ["whoami", getpass.getuser()],
            ["uname", platform.uname()[0]],
            [["cat", "/config.yaml", "/endpoint.log"], ""],
        ],
    ]
    cl = mocker.patch(f"{MOCK_DIAG_BASE}.general_commands_list")
    cl.return_value = general_report[0]

    yield general_report[1]


@pytest.fixture
def mock_all_reports(mock_hardware_report, mock_general_reports, mock_connection_tests):
    yield mock_hardware_report + mock_general_reports + mock_connection_tests


@pytest.fixture
def mock_connection_tests_only(mocker):
    mock_sock = mock.MagicMock()
    mock_ssock = mock.MagicMock()
    mock_ssock.cipher.return_value = ("mocked_cipher", "mocked_version", None)
    mock_create_conn = mocker.patch("socket.create_connection")
    mock_create_conn.return_value.__enter__.return_value = mock_sock
    mock_create_ctx = mocker.patch("ssl.create_default_context")
    mock_create_ctx.return_value.wrap_socket.return_value.__enter__.return_value = (
        mock_ssock
    )

    hl = mocker.patch(f"{MOCK_DIAG_BASE}.hardware_commands_list")
    hl.return_value = []
    gl = mocker.patch(f"{MOCK_DIAG_BASE}.general_commands_list")
    gl.return_value = []

    mocker.patch.object(WebClient, "get_version")


@pytest.fixture
def mock_endpoint_config_dir_data(randomstring, mock_gc_home, change_test_dir):
    ep_names = ["diagnostic_ep1", "diagnostic_other_ep"]

    config_random_data_by_file = {}

    for ep_name in ep_names:
        ep_dir_path = mock_gc_home / ep_name
        ep_dir_path.mkdir(exist_ok=False)

        conf_path = ep_dir_path / "config.yaml"
        log_path = ep_dir_path / "endpoint.log"
        conf_data = randomstring()
        log_data_random_part = randomstring()
        log_data_lines = [f"Fake logline #{i} for {ep_name}" for i in range(200)]
        log_data_lines.append(log_data_random_part)

        with open(conf_path, "w") as f:
            f.write(conf_data)

        with open(log_path, "w") as f:
            f.write("\n".join(log_data_lines))

        config_random_data_by_file[conf_path] = conf_data
        config_random_data_by_file[log_path] = log_data_random_part

    yield config_random_data_by_file


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
        cat([str(bfile)])()
    assert len(f.getvalue()) > 64


def test_cat_handles_binary_files_gracefully(fs):
    bfile = pathlib.Path("/some/binary/file_binary")
    bfile.parent.mkdir(parents=True)
    bfile.write_bytes(os.urandom(64))
    with contextlib.redirect_stdout(io.StringIO()) as f:
        cat([str(bfile)])()
    assert len(f.getvalue()) > 64


def test_cat_handles_missing_file_gracefully(fs):
    bfile = pathlib.Path("/some/file")
    bfile.parent.mkdir(parents=True)
    with contextlib.redirect_stdout(io.StringIO()) as f:
        cat([str(bfile)])()
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
            cat([str(bfile)])()
        else:
            cat([str(bfile)], max_bytes=max_bytes)()
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
        "GlobusComputeEngine/block-0/some-block-id/manager.log",
    ],
)
def test_cat_wildcard_finds_files_recursively(
    tmp_path,
    path,
    randomstring,
    change_test_dir,
):
    parent_path = tmp_path / "some_ep_dir"
    full_path = parent_path / path
    full_path.parent.mkdir(parents=True, exist_ok=True)
    assert parent_path.exists()
    contents = randomstring()
    with open(full_path, "w") as f:
        f.write(contents)

    with contextlib.redirect_stdout(io.StringIO()) as f:
        cat([str(parent_path) + "/**/*.log"], wildcard=True)()

    payload = str(f.getvalue())
    assert path in payload
    assert contents in payload


def test_diagnostic_simple(
    mock_gc_home,
    mock_all_reports,
    mock_gce_not_installed,
    mock_endpoint_config_dir_data,
    change_test_dir,
    capsys,
):
    do_diagnostic_base(DIAG_PRINT_ARGS + ["-k", "1"])
    captured = capsys.readouterr()

    assert captured.out.count("== Diagnostic") == len(mock_all_reports)
    for titles, expected_output in mock_all_reports:
        if expected_output:
            assert expected_output in captured.out
        title_parts = [titles] if isinstance(titles, str) else titles
        for title in title_parts:
            assert title in captured.out

    for random_data in mock_endpoint_config_dir_data.values():
        assert random_data in captured.out


@pytest.mark.parametrize("env", ["sandbox", "integration", "staging"])
def test_diagnostic_sdk_environment(
    mocker: MockFixture,
    monkeypatch,
    change_test_dir,
    mock_connection_tests_only,
    mock_gce_not_installed,
    env: str,
):
    mock_test_conn = mocker.patch(
        f"{MOCK_DIAG_BASE}.test_conn",
        return_value=lambda: "Success!",
    )
    mock_test_ssl_conn = mocker.patch(
        f"{MOCK_DIAG_BASE}.test_ssl_conn",
        return_value=lambda: "Success!",
    )

    monkeypatch.setenv("GLOBUS_SDK_ENVIRONMENT", env)

    do_diagnostic_base(DIAG_PRINT_ARGS + ["-k", "0"])

    assert f"compute.api.{env}.globuscs.info" in mock_test_conn.call_args_list[0][0]
    assert f"compute.amqps.{env}.globuscs.info" in mock_test_conn.call_args_list[1][0]
    assert f"compute.api.{env}.globuscs.info" in mock_test_ssl_conn.call_args_list[0][0]
    assert (
        f"compute.amqps.{env}.globuscs.info" in mock_test_ssl_conn.call_args_list[1][0]
    )


def test_diagnostic_gzip(
    mock_gc_home,
    mock_gce_not_installed,
    change_test_dir,
    mock_all_reports,
    mock_endpoint_config_dir_data,
    capsys,
):
    do_diagnostic_base(DIAG_ZIP_ARGS + ["-k", "1"])
    captured = capsys.readouterr()

    for fname in os.listdir(change_test_dir):
        if fname.endswith(".txt.gz"):
            break
    with gzip.open(fname, "rb") as f:
        contents = f.read().decode("utf-8")

    for line in captured.out.split("\n"):
        # All lines are diagnostic headings or blank lines for separation
        # No output -p flag was specified
        assert len(line.strip()) == 0 or line.startswith("== Diagnostic:")

    for random_file_data in mock_endpoint_config_dir_data.values():
        assert random_file_data in contents

    assert contents.count("== Diagnostic:") == len(mock_all_reports)


def test_diagnostic_log_size_limit(
    change_test_dir,
    mock_all_reports,
    mock_endpoint_config_dir_data,
    mock_gc_home,
):
    # Limit log file size to 2 KB, so the last 2KB changes but size is the same
    constant_diag_args = DIAG_ZIP_ARGS + ["--log-kb", "2"]
    do_diagnostic_base(constant_diag_args)

    fname = next(change_test_dir.glob("*.txt.gz"))
    with gzip.open(fname, "r") as f:
        first_iteration_output = f.read().decode("utf-8")

    fname.unlink()

    appended_to_log = False
    log_extra = "Some more log data blah blah 1 2 3"
    for log_file in mock_endpoint_config_dir_data.keys():
        if str(log_file).endswith(".log"):
            with open(log_file, "a") as f:
                # Write more than a few words just to make sure
                for _ in range(3):
                    f.write(log_extra + "\n")
            appended_to_log = True
    assert appended_to_log

    # Gather diags with logs again after appending to logs
    do_diagnostic_base(constant_diag_args)

    # Search for the file again, should be only 1
    for fname in os.listdir(change_test_dir):
        if fname.endswith(".txt.gz"):
            break
    with gzip.open(fname, "r") as f:
        second_iteration_output = f.read().decode("utf-8")

    assert log_extra in second_iteration_output

    # Need a little fudge factor as appending '| ' to logs and line breaks
    # sometimes produce a few more/less chars
    assert abs(len(first_iteration_output) - len(second_iteration_output)) < 10
