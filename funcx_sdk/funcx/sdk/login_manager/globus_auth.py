import os

import globus_sdk


def internal_auth_client():
    """
    This is the client that represents the FuncX application itself
    """

    client_id = os.environ.get(
        "FUNCX_SDK_CLIENT_ID", "4cf29807-cf21-49ec-9443-ff9a3fb9f81c"
    )
    return globus_sdk.NativeAppAuthClient(client_id, app_name="FuncX (internal client)")
