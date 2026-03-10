from __future__ import annotations

import os
from pathlib import Path

def get_compute_dir() -> Path:
    """
    Gets the base Compute directory.  Note that this merely returns the
    expected path but does not create it if it doesn't exist, like
    ensure_compute_dir() below.
    """
    # This is either set by the user or by the cli argument --config-dir
    env_dir = os.getenv("GLOBUS_COMPUTE_USER_DIR")
    if env_dir:
        return Path(env_dir)
    else:
        # Default to $HOME/.globus_compute
        return Path.home() / ".globus_compute"


def ensure_compute_dir() -> Path:
    compute_dir = get_compute_dir()

    if compute_dir.is_file():
        raise FileExistsError(
            f"Error creating directory {compute_dir}, "
            "please remove or rename the conflicting file"
        )
    elif not compute_dir.exists():
        compute_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    return compute_dir
