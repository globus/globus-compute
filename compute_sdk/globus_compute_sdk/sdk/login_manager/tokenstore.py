from __future__ import annotations

import os

from globus_sdk.tokenstorage import SQLiteAdapter

from .._environments import _get_envname
from ..compute_dir import ensure_compute_dir
from .client_login import get_client_login, is_client_login


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
