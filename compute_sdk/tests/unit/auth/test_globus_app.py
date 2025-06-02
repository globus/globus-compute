from __future__ import annotations

import pytest
from globus_compute_sdk.sdk.auth.globus_app import DEFAULT_CLIENT_ID, get_globus_app
from globus_sdk import ClientApp, UserApp
from pytest_mock import MockerFixture

_MOCK_BASE = "globus_compute_sdk.sdk.auth.globus_app."


@pytest.mark.parametrize(
    "client_id,client_secret", [(None, None), ("123", None), ("123", "456")]
)
def test_get_globus_app(
    client_id: str | None, client_secret: str | None, mocker: MockerFixture
):
    mocker.patch(
        f"{_MOCK_BASE}get_client_creds", return_value=(client_id, client_secret)
    )

    app = get_globus_app()

    assert app.config.request_refresh_tokens

    if client_id and client_secret:
        assert isinstance(app, ClientApp)
    else:
        assert isinstance(app, UserApp)

    if client_id:
        assert app.client_id == client_id
    else:
        assert app.client_id == DEFAULT_CLIENT_ID


@pytest.mark.parametrize("env_arg", ["some-arg", None])
@pytest.mark.parametrize("env_var", ["some-var", None])
def test_get_globus_app_with_environment(
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    env_arg: str | None,
    env_var: str | None,
):
    mock_get_token_storage = mocker.patch(f"{_MOCK_BASE}get_token_storage")
    mocker.patch(f"{_MOCK_BASE}UserApp", autospec=True)

    if env_var:
        monkeypatch.setenv("GLOBUS_SDK_ENVIRONMENT", env_var)

    get_globus_app(environment=env_arg)

    env = env_arg or env_var
    mock_get_token_storage.assert_called_once_with(environment=env)


def test_client_app_requires_creds(mocker: MockerFixture):
    mocker.patch(f"{_MOCK_BASE}get_client_creds", return_value=(None, "456"))
    with pytest.raises(ValueError) as err:
        get_globus_app()
    assert "GLOBUS_COMPUTE_CLIENT_SECRET must be set" in str(err.value)
