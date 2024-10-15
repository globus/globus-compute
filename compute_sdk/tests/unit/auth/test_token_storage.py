from __future__ import annotations

import pathlib

import pytest
from globus_compute_sdk.sdk.auth.token_storage import (
    _resolve_namespace,
    get_token_storage,
)
from globus_sdk.tokenstorage import SQLiteTokenStorage
from pytest_mock import MockerFixture

_MOCK_BASE = "globus_compute_sdk.sdk.auth.token_storage."


@pytest.mark.parametrize("env", ("foo-env", None))
@pytest.mark.parametrize("env_var", ("foo-env-var", "production"))
@pytest.mark.parametrize("client_id", ("foo-client-id", None))
@pytest.mark.parametrize("client_secret", ("foo-client-secret", None))
def test_resolve_namespace(
    env: str | None,
    env_var: str | None,
    client_id: str | None,
    client_secret: str | None,
    mocker: MockerFixture,
):
    mocker.patch(f"{_MOCK_BASE}_get_envname", return_value=env_var)
    mocker.patch(
        f"{_MOCK_BASE}get_client_creds", return_value=(client_id, client_secret)
    )

    namespace = _resolve_namespace(env)

    env = env or env_var
    if client_id and client_secret:
        assert namespace == f"clientprofile/{env}/{client_id}"
    else:
        assert namespace == f"user/{env}"


def test_get_token_storage(tmp_path: pathlib.Path, mocker: MockerFixture, randomstring):
    env = randomstring()
    namespace = f"foo/{env}"
    compute_dir = tmp_path / ".globus_compute"
    mock_resolve_namespace = mocker.patch(
        f"{_MOCK_BASE}_resolve_namespace", return_value=namespace
    )
    mocker.patch(f"{_MOCK_BASE}ensure_compute_dir", return_value=compute_dir)

    storage = get_token_storage(environment=env)

    assert isinstance(storage, SQLiteTokenStorage)
    assert storage.filepath == f"{compute_dir}/storage.db"
    mock_resolve_namespace.assert_called_once_with(env)
    assert storage.namespace == namespace
