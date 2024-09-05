from __future__ import annotations

from ..utils import get_env_var_with_deprecation


def get_client_creds() -> tuple[str | None, str | None]:
    client_id = get_env_var_with_deprecation(
        "GLOBUS_COMPUTE_CLIENT_ID", "FUNCX_SDK_CLIENT_ID"
    )
    client_secret = get_env_var_with_deprecation(
        "GLOBUS_COMPUTE_CLIENT_SECRET", "FUNCX_SDK_CLIENT_SECRET"
    )
    return client_id, client_secret
