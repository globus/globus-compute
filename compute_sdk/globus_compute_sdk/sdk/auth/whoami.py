from __future__ import annotations

import logging

from globus_compute_sdk.sdk.auth.auth_client import ComputeAuthClient
from globus_compute_sdk.sdk.utils.printing import print_table
from globus_sdk import AuthAPIError, GlobusApp

NOT_LOGGED_IN_MSG = "Unable to retrieve user information. Please log in again."

logger = logging.getLogger(__name__)


def get_user_info(auth_client: ComputeAuthClient, user_id: str) -> list[str]:
    """
    Parse username, name, email from auth response and return a list of info
    """
    user_info = auth_client.get_identities(ids=user_id)
    return [
        user_info["identities"][0]["username"],
        user_info["identities"][0]["name"],
        user_id,
        user_info["identities"][0]["email"],
    ]


def print_whoami_info(app: GlobusApp, linked_identities: bool = False) -> None:
    """
    Display information for the currently logged-in user.
    """
    if app.login_required():
        logger.debug(NOT_LOGGED_IN_MSG, exc_info=True)
        raise ValueError(NOT_LOGGED_IN_MSG)

    auth_client = ComputeAuthClient(app=app)
    whoami_headers = ["Username", "Name", "ID", "Email"]

    # get userinfo from auth.
    # if we get back an error the user likely needs to log in again
    try:
        user_info = {}
        res = auth_client.oauth2_userinfo()
        main_id = res["sub"]
        user_info[main_id] = get_user_info(auth_client, main_id)

        whoami_rows = []
        if linked_identities:
            if "identity_set" not in res:
                raise ValueError(
                    "Your current login does not have the consents required "
                    "to view your full identity set. Please log in again "
                    "to agree to the required consents.\n\n"
                    "  Hint: use the --logout flag"
                )

            for linked in res["identity_set"]:
                linked_id = linked["sub"]
                if linked_id not in user_info:
                    user_info[linked_id] = get_user_info(auth_client, linked_id)
                whoami_rows.append(user_info[linked_id])
        else:
            whoami_rows.append(user_info[main_id])

        print_table(whoami_headers, whoami_rows)
    except AuthAPIError:
        raise ValueError(NOT_LOGGED_IN_MSG)
