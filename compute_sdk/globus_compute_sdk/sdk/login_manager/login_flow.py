from __future__ import annotations

import platform

from .globus_auth import internal_auth_client


def do_link_auth_flow(scopes: list[str]):
    auth_client = internal_auth_client()

    # start the Confidential App Grant flow
    auth_client.oauth2_start_flow(
        redirect_uri=auth_client.base_url + "v2/web/auth-code",
        refresh_tokens=True,
        requested_scopes=scopes,
        prefill_named_grant=platform.node(),
    )

    # prompt
    query_params = {"prompt": "login"}
    linkprompt = "Please authenticate with Globus here"
    print(
        "{0}:\n{1}\n{2}\n{1}\n".format(
            linkprompt,
            "-" * len(linkprompt),
            auth_client.oauth2_get_authorize_url(query_params=query_params),
        )
    )

    # come back with auth code
    auth_code = input("Enter the resulting Authorization Code here: ").strip()

    # finish auth flow
    return auth_client.oauth2_exchange_code_for_tokens(auth_code)
