from __future__ import annotations

import os
from pathlib import Path


def _home() -> Path:
    # this is a hook point for tests to patch over
    # it just returns `pathlib.Path.home()`
    # replace this with a mock to return some test directory
    return Path.home()


def ensure_compute_dir(home: Path | None = None) -> Path:
    dirname = (home if home else _home()) / ".globus_compute"

    user_dir = os.getenv("GLOBUS_COMPUTE_USER_DIR")
    if user_dir:
        dirname = Path(user_dir)

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
