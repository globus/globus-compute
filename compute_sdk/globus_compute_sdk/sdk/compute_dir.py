from __future__ import annotations

import os
import pathlib


def get_compute_dir() -> pathlib.Path:
    if user_dir := os.getenv("GLOBUS_COMPUTE_USER_DIR"):
        return pathlib.Path(user_dir)
    return pathlib.Path.home() / ".globus_compute"


def ensure_compute_dir() -> pathlib.Path:
    dirname = get_compute_dir()

    if not dirname.exists():
        dirname.mkdir(mode=0o700, parents=True, exist_ok=True)

    if not dirname.is_dir():
        raise NotADirectoryError(f"Failed to create directory: {dirname}\n")

    return dirname
