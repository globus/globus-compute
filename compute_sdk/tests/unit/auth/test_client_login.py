from __future__ import annotations

import pytest
from globus_compute_sdk.sdk.auth.client_login import get_client_creds


@pytest.mark.parametrize("client_id", ["foo", "", None])
@pytest.mark.parametrize("client_secret", ["foo", "", None])
def test_get_client_creds(
    client_id: str | None, client_secret: str | None, monkeypatch: pytest.MonkeyPatch
):
    if client_id is not None:
        monkeypatch.setenv("GLOBUS_COMPUTE_CLIENT_ID", client_id)
    if client_secret is not None:
        monkeypatch.setenv("GLOBUS_COMPUTE_CLIENT_SECRET", client_secret)
    assert get_client_creds() == (client_id, client_secret)


@pytest.mark.parametrize("env_var", ["FUNCX_SDK_CLIENT_ID", "FUNCX_SDK_CLIENT_SECRET"])
def test_get_client_creds_deprecated_env_var(
    env_var: str, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv(env_var, "foo")
    with pytest.warns(UserWarning) as record:
        get_client_creds()
    assert any(env_var in str(r.message) for r in record)
