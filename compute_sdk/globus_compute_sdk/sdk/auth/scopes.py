from __future__ import annotations

from globus_sdk.scopes import ScopeBuilder

from ..utils import get_env_var_with_deprecation

DEFAULT_SCOPE = (
    "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
)


class ComputeScopeBuilder(ScopeBuilder):
    # A ScopeBuilder in the style of globus_sdk.scopes for the Globus Compute service
    # that supports one scope named 'all', as in ``ComputeScopes.all``
    def __init__(self):
        # FIXME:
        # For some reason, the Compute resource server name on the production scope is
        # "funcx_service" even though this doesn't match the resource server ID and the
        # scope is in URL format
        # At some point, we ought to work out how to normalize this so that it conforms
        # to one of the known, pre-existing modes for scopes
        super().__init__(resource_server="funcx_service")
        self.all = get_env_var_with_deprecation(
            "GLOBUS_COMPUTE_SCOPE",
            "FUNCX_SCOPE",
            DEFAULT_SCOPE,
        )


ComputeScopes = ComputeScopeBuilder()
