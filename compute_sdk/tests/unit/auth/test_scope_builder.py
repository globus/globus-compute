from __future__ import annotations

import pytest
from globus_compute_sdk.sdk.auth.scopes import DEFAULT_SCOPE, ComputeScopeBuilder


@pytest.mark.parametrize("env_var_val", ["some-scope", None])
def test_scope_builder(env_var_val: str | None, monkeypatch: pytest.MonkeyPatch):
    if env_var_val is not None:
        monkeypatch.setenv("GLOBUS_COMPUTE_SCOPE", env_var_val)

    sb = ComputeScopeBuilder()

    assert sb.resource_server == "funcx_service"
    if env_var_val:
        assert sb.all == env_var_val
    else:
        assert sb.all == DEFAULT_SCOPE
