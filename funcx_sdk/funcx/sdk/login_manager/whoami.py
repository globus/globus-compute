from datetime import datetime

from globus_sdk import AuthAPIError

from funcx.sdk.login_manager import LoginManager
from funcx.sdk.utils.printing import print_table

NOT_LOGGED_IN_MSG = "Unable to retrieve user information. Please log in again."


def get_user_info(auth_client, user_id, epoch_auth):
    """
    Parse username, name, and last authed time from response
    """
    user_info = auth_client.get_identities(ids=user_id)
    return [
        user_info["identities"][0]["username"],
        user_info["identities"][0]["name"],
        datetime.fromtimestamp(epoch_auth).astimezone().isoformat(),
    ]


def print_whoami_info(linked_identities: bool = False) -> None:
    """
    Display information for the currently logged-in user.
    """

    try:
        auth_client = LoginManager().get_auth_client()
    except LookupError:
        raise ValueError(NOT_LOGGED_IN_MSG)

    whoami_headers = ["Username", "Name", "ID", "Auth Time"]

    # get userinfo from auth.
    # if we get back an error the user likely needs to log in again
    try:
        user_info = {}
        res = auth_client.oauth2_userinfo()
        main_id = res["sub"]
        user_info[main_id] = get_user_info(
            auth_client, main_id, res["last_authentication"]
        )

        if linked_identities:
            whoami_rows = []
            if "identity_set" not in res:
                raise ValueError(
                    "Your current login does not have the consents required "
                    "to view your full identity set. Please log in again "
                    "to agree to the required consents."
                )

            for linked in res["identity_set"]:
                linked_id = linked["sub"]
                if linked_id not in user_info:
                    user_info[linked_id] = get_user_info(
                        auth_client,
                        linked_id,
                        linked["last_authentication"],
                    )
                whoami_rows.append(
                    [
                        user_info[linked_id][0],
                        user_info[linked_id][1],
                        main_id,
                        user_info[linked_id][2],
                    ]
                )
            print_table(whoami_headers, whoami_rows)
        else:
            print_table(
                whoami_headers,
                [
                    [
                        user_info[main_id][0],
                        user_info[main_id][1],
                        main_id,
                        user_info[main_id][2],
                    ],
                ],
            )
    except AuthAPIError:
        raise ValueError(NOT_LOGGED_IN_MSG)
