from __future__ import annotations

import os
import pathlib

import pytest
from globus_compute_sdk.sdk.compute_dir import ensure_compute_dir
from pyfakefs.fake_filesystem import FakeFilesystem


@pytest.mark.parametrize("dir_exists", [True, False])
@pytest.mark.parametrize("env_dir", ["/my/dir", "", None])
def test_ensure_compute_dir(
    dir_exists: bool,
    env_dir: str | None,
    fs: FakeFilesystem,
    monkeypatch: pytest.MonkeyPatch,
):
    home = pathlib.Path.home()

    expected_dirname = home / ".globus_compute"

    if dir_exists:
        fs.create_dir(expected_dirname)

    if env_dir is not None:
        # Override ~/.globus_compute if env var is set
        dirname = pathlib.Path(env_dir)
        monkeypatch.setenv("GLOBUS_COMPUTE_USER_DIR", str(dirname))
        expected_dirname = pathlib.Path(env_dir)
    else:
        monkeypatch.delenv("GLOBUS_COMPUTE_USER_DIR", raising=False)

    actual_compute_dir = ensure_compute_dir()

    assert actual_compute_dir.is_dir()
    assert actual_compute_dir.resolve() == expected_dirname.resolve()


@pytest.mark.parametrize("user_dir_defined", [True, False])
def test_conflicting_compute_file(
    user_dir_defined: bool, fs: FakeFilesystem, monkeypatch: pytest.MonkeyPatch
):
    filename = pathlib.Path.home() / ".globus_compute"
    fs.create_file(filename)

    with pytest.raises(FileExistsError) as exc:
        if user_dir_defined:
            monkeypatch.setenv("GLOBUS_COMPUTE_USER_DIR", str(filename))
        else:
            monkeypatch.delenv("GLOBUS_COMPUTE_USER_DIR", raising=False)
        ensure_compute_dir()

    assert "Error creating directory" in str(exc)


def test_restricted_user_dir(fs: FakeFilesystem, monkeypatch: pytest.MonkeyPatch):
    parent_dirname = pathlib.Path("/parent/dir/")
    compute_dirname = parent_dirname / "compute"

    fs.create_dir(parent_dirname)
    os.chmod(parent_dirname, 0o000)

    with pytest.raises(PermissionError) as exc:
        monkeypatch.setenv("GLOBUS_COMPUTE_USER_DIR", str(compute_dirname))
        ensure_compute_dir()

    assert "Permission denied" in str(exc)
