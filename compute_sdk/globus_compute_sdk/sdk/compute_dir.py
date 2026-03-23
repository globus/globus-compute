from __future__ import annotations

import os
from pathlib import Path


def get_compute_dir() -> Path:
    """
    Gets the base Compute directory.
      Note this merely returns the expected path and does not create
      the directory if it doesn't exist or set env var, unlike ensure_compute_dir
    """
    # This is either set by the user via GLOBUS_COMPUTE_USER_DIR
    #  or by the cli argument --config-dir via overriding the env var
    if env_dir := os.environ.get("GLOBUS_COMPUTE_USER_DIR"):
        return Path(env_dir)
    else:
        return Path.home() / ".globus_compute"


def ensure_compute_dir(config_path_str: str | None = None) -> Path:
    """
    Creates the base Compute directory if it doesn't already exist.

    :param config_path_str: Force the compute base dir to be this location
                              by overwriting the current GLOBUS_COMPUTE_USER_DIR
    """

    if config_path_str is not None:
        if not config_path_str.strip():
            raise ValueError("Config directory argument cannot be empty")
        os.environ["GLOBUS_COMPUTE_USER_DIR"] = config_path_str

    compute_dir = get_compute_dir()
    if compute_dir.is_file():
        raise FileExistsError(
            f"Error creating directory {compute_dir}, "
            "please remove or rename the conflicting file"
        )
    elif not compute_dir.exists():
        compute_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    return compute_dir
