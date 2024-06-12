from __future__ import annotations

import os
import pathlib

import pytest
from globus_compute_sdk.sdk.compute_dir import ensure_compute_dir
from pyfakefs.fake_filesystem import FakeFilesystem


@pytest.mark.parametrize("dir_exists", [True, False])
@pytest.mark.parametrize("user_dir", ["/my/dir", None, ""])
@pytest.mark.parametrize("home_arg", ["/other/dir", None])
def test_ensure_compute_dir(
    dir_exists: bool,
    user_dir: str | None,
    home_arg: str | None,
    fs: FakeFilesystem,
    monkeypatch: pytest.MonkeyPatch,
):
    if home_arg and not user_dir:
        home = pathlib.Path(home_arg)
    else:
        home = pathlib.Path.home()

    dirname = home / ".globus_compute"

    if dir_exists:
        fs.create_dir(dirname)

    if user_dir is not None:
        dirname = pathlib.Path(user_dir)
        monkeypatch.setenv("GLOBUS_COMPUTE_USER_DIR", str(dirname))

    compute_dirname = ensure_compute_dir(home=home)

    assert compute_dirname.is_dir()
    assert compute_dirname == dirname


@pytest.mark.parametrize("user_dir_defined", [True, False])
def test_conflicting_compute_file(
    user_dir_defined: bool, fs: FakeFilesystem, monkeypatch: pytest.MonkeyPatch
):
    filename = pathlib.Path.home() / ".globus_compute"
    fs.create_file(filename)

    with pytest.raises(FileExistsError) as exc:
        if user_dir_defined:
            monkeypatch.setenv("GLOBUS_COMPUTE_USER_DIR", str(filename))
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
