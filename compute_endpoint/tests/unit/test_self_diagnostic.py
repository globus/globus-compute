import contextlib
import io
import os
import pathlib

import pytest
from globus_compute_endpoint.self_diagnostic import cat


def test_cat_handles_binary_files_gracefully(fs):
    bfile = pathlib.Path("/some/binary/file")
    bfile.parent.mkdir(parents=True)
    bfile.write_bytes(os.urandom(64))
    with contextlib.redirect_stdout(io.BytesIO()) as f:
        cat(str(bfile))()
    assert len(f.getvalue()) > 64


def test_cat_handles_missing_file_gracefully(fs):
    bfile = pathlib.Path("/some/file")
    bfile.parent.mkdir(parents=True)
    with contextlib.redirect_stdout(io.BytesIO()) as f:
        cat(str(bfile))()
    fname = str(bfile).encode()
    expected_header = b"cat " + fname
    expected_header_hline = b"=" * len(str(bfile))
    expected_footer_hline = b"-" * len(str(bfile))
    assert expected_header in f.getvalue()
    assert expected_header_hline in f.getvalue()
    assert expected_footer_hline in f.getvalue()
    assert b"\nNo file named " + fname in f.getvalue()


@pytest.mark.parametrize("max_bytes", (-64, -32, -1, None, 0, 1, 32, 64))
def test_cat_honors_max_bytes(fs, max_bytes):
    raw_file_size = 100
    expected_sentinel = b"abcde12345"
    bfile = pathlib.Path("/some/binary/file")
    bfile.parent.mkdir(parents=True)

    assert raw_file_size > (max_bytes or 0), "Verify test setup"
    fdata = expected_sentinel + os.urandom(raw_file_size)
    fdata = fdata.replace(b"\n", b"X")  # test splits on newline
    fdata = fdata.replace(b"\r", b"Y")  # test splits on newline
    bfile.write_bytes(fdata)

    fsize = len(bfile.read_bytes())
    if max_bytes:
        assert fsize > max_bytes, "Verify test setup: file size bigger than test's max"

    with contextlib.redirect_stdout(io.BytesIO()) as f:
        if max_bytes is None:
            cat(str(bfile))()
        else:
            cat(str(bfile), max_bytes=max_bytes)()
    payload = b"\n".join(f.getvalue().splitlines()[2:-2])  # remove header and footer
    payload = payload[3:]  # remove line header

    if max_bytes and max_bytes > 0:
        assert len(payload) <= max_bytes, fdata
        assert expected_sentinel not in payload, fdata
    else:
        assert len(payload) == fsize, fdata
        assert expected_sentinel in payload, fdata


@pytest.mark.parametrize(
    "path",
    [
        "endpoint.log",
        "GlobusComputeEngine/interchange.log",
        "GlobusComputeEngine/block-0/some-block-id/manager.log",
        "GlobusComputeEngine/block-0/some-block-id/worker_0.log",
    ],
)
def test_cat_wildcard_finds_files_recursively(fs, path, randomstring):
    full_path = pathlib.Path("/some_ep_dir/" + path)
    full_path.parent.mkdir(parents=True, exist_ok=True)
    contents = randomstring()
    full_path.write_text(contents)

    with contextlib.redirect_stdout(io.BytesIO()) as f:
        cat("/some_ep_dir/**/*.log", wildcard=True)()

    payload = str(f.getvalue())
    assert path in payload
    assert contents in payload
