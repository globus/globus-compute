from __future__ import annotations

import os
import pathlib


def ensure_compute_dir() -> pathlib.Path:
    dirname = pathlib.Path.home() / ".globus_compute"

    user_dir = os.getenv("GLOBUS_COMPUTE_USER_DIR")
    if user_dir:
        dirname = pathlib.Path(user_dir)

    if dirname.is_dir():
        pass
    elif dirname.is_file():
        raise FileExistsError(
            f"Error creating directory {dirname}, "
            "please remove or rename the conflicting file"
        )
    else:
        dirname.mkdir(mode=0o700, parents=True, exist_ok=True)

    return dirname
