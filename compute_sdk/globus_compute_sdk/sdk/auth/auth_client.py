from __future__ import annotations

from globus_sdk import AuthClient
from globus_sdk.scopes import AuthScopes, Scope


class ComputeAuthClient(AuthClient):
    default_scope_requirements = [
        Scope(AuthScopes.openid.scope_string),
        Scope(AuthScopes.manage_projects.scope_string),
    ]
