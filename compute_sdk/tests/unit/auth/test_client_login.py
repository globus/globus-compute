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
