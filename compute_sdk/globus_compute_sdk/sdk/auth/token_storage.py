from __future__ import annotations

import os

from globus_sdk.tokenstorage import SQLiteTokenStorage

from .._environments import _get_envname
from ..compute_dir import ensure_compute_dir
from .client_login import get_client_creds


def _get_storage_filepath():
    compute_dir = ensure_compute_dir()
    return os.path.join(compute_dir, "storage.db")


def _resolve_namespace(environment: str | None = None) -> str:
    """Return the namespace used to save tokens. This will check if a
    client login is being used and return either `user/<envname>` or
    `clientprofile/<envname>/<clientid>`.
    """
    env = environment if environment is not None else _get_envname()

    client_id, client_secret = get_client_creds()
    if client_id and client_secret:
        return f"clientprofile/{env}/{client_id}"

    return f"user/{env}"


def get_token_storage(environment: str | None = None) -> SQLiteTokenStorage:
    return SQLiteTokenStorage(
        filepath=_get_storage_filepath(),
        namespace=_resolve_namespace(environment),
        connect_params={"check_same_thread": False},
    )
