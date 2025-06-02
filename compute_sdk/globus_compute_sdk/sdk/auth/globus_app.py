from __future__ import annotations

import os
import platform

from globus_sdk import ClientApp, GlobusApp, GlobusAppConfig, UserApp

from .client_login import get_client_creds
from .token_storage import get_token_storage

DEFAULT_CLIENT_ID = "4cf29807-cf21-49ec-9443-ff9a3fb9f81c"


def get_globus_app(environment: str | None = None) -> GlobusApp:
    environment = environment or os.getenv("GLOBUS_SDK_ENVIRONMENT")
    app_name = platform.node()
    client_id, client_secret = get_client_creds()
    config = GlobusAppConfig(
        token_storage=get_token_storage(environment=environment),
        request_refresh_tokens=True,
    )

    if client_id and client_secret:
        return ClientApp(
            app_name=app_name,
            client_id=client_id,
            client_secret=client_secret,
            config=config,
        )

    elif client_secret:
        raise ValueError(
            "Both GLOBUS_COMPUTE_CLIENT_ID and GLOBUS_COMPUTE_CLIENT_SECRET must "
            "be set to use a client identity. Either set both environment "
            "variables, or unset them to use a normal login."
        )

    else:
        client_id = client_id or DEFAULT_CLIENT_ID
        return UserApp(app_name=app_name, client_id=client_id, config=config)
