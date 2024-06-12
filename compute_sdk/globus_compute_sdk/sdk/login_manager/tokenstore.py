from __future__ import annotations

import os
import pathlib

from globus_sdk.tokenstorage import SQLiteAdapter

from .._environments import _get_envname
from .client_login import get_client_login, is_client_login


def _home() -> pathlib.Path:
    # this is a hook point for tests to patch over
    # it just returns `pathlib.Path.home()`
    # replace this with a mock to return some test directory
    return pathlib.Path.home()


def ensure_compute_dir(home: os.PathLike | None = None) -> pathlib.Path:
    dirname = _home() / ".globus_compute"

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


def _get_storage_filename():
    datadir = ensure_compute_dir()
    return os.path.join(datadir, "storage.db")


def _resolve_namespace(environment: str | None) -> str:
    """
    Return the namespace used to save tokens. This will check
    if a client login is being used and return either:
      user/<envname>
    or
      clientprofile/<envname>/<clientid>

    e.g.

      user/production
    """
    env = environment if environment is not None else _get_envname()

    if is_client_login():
        client_id = get_client_login().client_id
        return f"clientprofile/{env}/{client_id}"

    return f"user/{env}"


def get_token_storage_adapter(*, environment: str | None = None) -> SQLiteAdapter:
    return SQLiteAdapter(
        _get_storage_filename(),
        namespace=_resolve_namespace(environment),  # current env == "namespace"
        connect_params={"check_same_thread": False},
    )
