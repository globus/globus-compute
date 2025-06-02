from click import ClickException
from globus_compute_sdk.sdk.auth.auth_client import ComputeAuthClient
from globus_compute_sdk.sdk.auth.globus_app import get_globus_app
from globus_sdk import ComputeClient, GlobusApp


def get_globus_app_with_scopes() -> GlobusApp:
    try:
        app = get_globus_app()
    except (RuntimeError, ValueError) as e:
        raise ClickException(str(e))
    app.add_scope_requirements(
        {
            ComputeClient.scopes.resource_server: ComputeClient.default_scope_requirements,  # noqa E501
            ComputeAuthClient.scopes.resource_server: ComputeAuthClient.default_scope_requirements,  # noqa E501
        }
    )
    return app
