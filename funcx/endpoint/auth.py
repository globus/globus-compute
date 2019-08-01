import globus_sdk

def load_auth_client():
    """Create an AuthClient for the portal

    No credentials are used if the server is not production

    Returns
    -------
    globus_sdk.ConfidentialAppAuthClient
        Client used to perform GlobusAuth actions
    """

    _prod = True

    if _prod:
        app = globus_sdk.ConfidentialAppAuthClient(GLOBUS_CLIENT,
                                                   GLOBUS_KEY)
    else:
        app = globus_sdk.ConfidentialAppAuthClient('', '')
    return app
