from globus_compute_sdk.sdk.auth.auth_client import ComputeAuthClient
from globus_sdk.scopes import AuthScopes


def test_default_scope_requirements():
    client = ComputeAuthClient()
    expected_scopes = [AuthScopes.openid, AuthScopes.manage_projects]
    scopes = [str(s) for s in client.default_scope_requirements]
    assert len(expected_scopes) == len(scopes)
    for scope in expected_scopes:
        assert scope in scopes
