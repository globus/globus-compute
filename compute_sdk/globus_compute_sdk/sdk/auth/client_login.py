from __future__ import annotations

import os


def get_client_creds() -> tuple[str | None, str | None]:
    return os.getenv("GLOBUS_COMPUTE_CLIENT_ID"), os.getenv(
        "GLOBUS_COMPUTE_CLIENT_SECRET"
    )
