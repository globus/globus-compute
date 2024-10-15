from __future__ import annotations

import platform
import sys

from globus_sdk import ClientApp, GlobusAppConfig, UserApp

from .client_login import get_client_creds
from .token_storage import get_token_storage

DEFAULT_CLIENT_ID = "4cf29807-cf21-49ec-9443-ff9a3fb9f81c"


def _is_jupyter():
    # Simplest way to find out if we are in Jupyter without having to
    # check imports
    return "jupyter_core" in sys.modules


def get_globus_app(environment: str | None = None):
    app_name = platform.node()
    client_id, client_secret = get_client_creds()
    config = GlobusAppConfig(token_storage=get_token_storage(environment=environment))

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

    # The authorization-via-web-link flow requires stdin; the user must visit
    # the web link and enter generated code.
    elif (not sys.stdin.isatty() or sys.stdin.closed) and not _is_jupyter():
        # Not technically necessary; the login flow would just die with an EOF
        # during input(), but adding this message here is much more direct --
        # handle the non-happy path by letting the user know precisely the issue
        raise RuntimeError(
            "Unable to run native app login flow: stdin is closed or is not a TTY."
        )

    else:
        client_id = client_id or DEFAULT_CLIENT_ID
        return UserApp(app_name=app_name, client_id=client_id, config=config)
