from __future__ import annotations

import os
import platform

from globus_sdk import OAuthTokenResponse
from globus_sdk.gare import GlobusAuthorizationParameters

from .globus_auth import internal_auth_client


def do_link_auth_flow(auth_params: GlobusAuthorizationParameters) -> OAuthTokenResponse:
    auth_client = internal_auth_client()

    auth_client.oauth2_start_flow(
        redirect_uri=auth_client.base_url + "v2/web/auth-code",
        refresh_tokens=True,
        requested_scopes=auth_params.required_scopes,
        prefill_named_grant=f"{os.getlogin()} on {platform.node()}",
    )

    auth_url = auth_client.oauth2_get_authorize_url(
        session_required_identities=auth_params.session_required_identities,
        session_required_single_domain=auth_params.session_required_single_domain,
        session_required_policies=auth_params.session_required_policies,
        session_required_mfa=auth_params.session_required_mfa,
        session_message=auth_params.session_message,
        prompt="login",
    )
    prompt_text = "Please authenticate with Globus via the following URL:"
    sep = "-" * len(prompt_text)
    prompt_text += f"\n\n{sep}\n{auth_url}\n{sep}"
    prompt_text += "\n\nThen enter the resulting Authorization Code here: "

    # come back with auth code
    auth_code = input(prompt_text).strip()

    # finish auth flow
    return auth_client.oauth2_exchange_code_for_tokens(auth_code)
