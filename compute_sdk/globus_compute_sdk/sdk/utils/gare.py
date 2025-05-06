import typing as t

from globus_sdk import GlobusAPIError, GlobusApp
from globus_sdk.gare import to_gares


def gare_handler(app: GlobusApp, f: t.Callable, *args, **kwargs):
    try:
        return f(*args, **kwargs)
    except GlobusAPIError as e:
        gares = to_gares([e])
        if not gares:
            raise

        for gare in gares:
            auth_params = gare.authorization_parameters
            if auth_params.session_message is None:
                auth_params.session_message = gare.extra.get("reason")

            app.login(auth_params=auth_params)

        return f(*args, **kwargs)
