"""Utilities related to GlobusAuth"""


import platform
import globus_sdk

from globus_sdk import RefreshTokenAuthorizer
from funcx.sdk.config import (internal_auth_client, safeprint, lookup_option, write_option,
                              remove_option, check_logged_in,
                              FUNCX_RT_OPTNAME, FUNCX_AT_OPTNAME, FUNCX_AT_EXPIRES_OPTNAME)


def do_login_flow():
    """
    Do the globus native client login flow.

    Saves the token received by the client to the Globus configuration file
    """

    native_client = internal_auth_client()

    label = platform.node() or None

    FUNCX_SCOPE = 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all'

    native_client.oauth2_start_flow(
        requested_scopes=FUNCX_SCOPE,
        refresh_tokens=True, prefill_named_grant=label)
    linkprompt = 'Please log into Globus here'
    safeprint('{0}:\n{1}\n{2}\n{1}\n'
              .format(linkprompt, '-' * len(linkprompt),
                      native_client.oauth2_get_authorize_url()))
    auth_code = input(
        'Enter the resulting Authorization Code here:\n').strip()
    tkn = native_client.oauth2_exchange_code_for_tokens(auth_code)
    _revoke_current_tokens(native_client)
    _store_config(tkn)


def make_authorizer():
    """
    Make a RefreshTokenAuthorizer given the tokens stored on disk

    Returns:
        (RefreshTokenAuthorizer): Tool to generate authorization credentials
    """

    if not check_logged_in():
        safeprint("No authorization credentials present. You must log in")
        do_login_flow()

    # Get the authorization client
    auth_client = internal_auth_client()

    # Get the tokens needed by the service
    rf_token = lookup_option(FUNCX_RT_OPTNAME)
    at_token = lookup_option(FUNCX_AT_OPTNAME)
    at_expires = int(lookup_option(FUNCX_AT_EXPIRES_OPTNAME))
    authorizer = RefreshTokenAuthorizer(rf_token, auth_client, access_token=at_token,
                                        expires_at=at_expires)

    return authorizer


def logout():
    """
    Remove Globus credentials from configuration file and revokes their authorization
    """

    native_client = internal_auth_client()

    # remove tokens from config and revoke them
    # also, track whether or not we should print the rescind help
    for token_opt in (FUNCX_RT_OPTNAME, FUNCX_AT_OPTNAME):
        # first lookup the token -- if not found we'll continue
        token = lookup_option(token_opt)
        if not token:
            safeprint(('Warning: Found no token named "{}"! '
                       'Recommend rescinding consent').format(token_opt))
            continue
        # token was found, so try to revoke it
        try:
            native_client.oauth2_revoke_token(token)
        # if we network error, revocation failed -- print message and abort so
        # that we can revoke later when the network is working
        except globus_sdk.NetworkError:
            safeprint(('Failed to reach Globus to revoke tokens. '
                       'Because we cannot revoke these tokens, cancelling '
                       'logout'))
            return
        # finally, we revoked, so it's safe to remove the token
        remove_option(token_opt)

    # remove expiration time, just for cleanliness
    remove_option(FUNCX_AT_EXPIRES_OPTNAME)


def _revoke_current_tokens(native_client):
    """
    Revoke the tokens associated with a particular scope

    Args:
         native_client (NativeAppAuthClient): Authorization client for scope to be cleared
    """
    for token_opt in (FUNCX_RT_OPTNAME, FUNCX_AT_OPTNAME):
        token = lookup_option(token_opt)
        if token:
            native_client.oauth2_revoke_token(token)


def _store_config(token_response):
    """
    Store the tokens on disk.

    Args:
        token_response (OAuthTokenResponse): Response from a token request
    """
    tkn = token_response.by_resource_server

    funcx_at = tkn['funcx_service']['access_token']
    funcx_rt = tkn['funcx_service']['refresh_token']
    funcx_at_expires = tkn['funcx_service']['expires_at_seconds']

    write_option(FUNCX_RT_OPTNAME, funcx_rt)
    write_option(FUNCX_AT_OPTNAME, funcx_at)
    write_option(FUNCX_AT_EXPIRES_OPTNAME, funcx_at_expires)
