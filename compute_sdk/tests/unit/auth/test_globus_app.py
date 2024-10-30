from __future__ import annotations

import pytest
from globus_compute_sdk.sdk.auth.globus_app import DEFAULT_CLIENT_ID, get_globus_app
from globus_sdk import ClientApp, GlobusApp, UserApp
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
    mock_stdin = mocker.patch(f"{_MOCK_BASE}sys.stdin")
    mock_stdin.isatty.return_value = True
    mock_stdin.closed = False

    app = get_globus_app()

    if client_id and client_secret:
        assert isinstance(app, ClientApp)
    else:
        assert isinstance(app, UserApp)

    if client_id:
        assert app.client_id == client_id
    else:
        assert app.client_id == DEFAULT_CLIENT_ID


def test_get_globus_app_with_environment(mocker: MockerFixture, randomstring):
    mock_get_token_storage = mocker.patch(f"{_MOCK_BASE}get_token_storage")
    mocker.patch(f"{_MOCK_BASE}UserApp", autospec=True)
    mock_stdin = mocker.patch(f"{_MOCK_BASE}sys.stdin")
    mock_stdin.isatty.return_value = True
    mock_stdin.closed = False

    env = randomstring()
    get_globus_app(environment=env)

    mock_get_token_storage.assert_called_once_with(environment=env)


def test_client_app_requires_creds(mocker: MockerFixture):
    mocker.patch(f"{_MOCK_BASE}get_client_creds", return_value=(None, "456"))
    with pytest.raises(ValueError) as err:
        get_globus_app()
    assert "GLOBUS_COMPUTE_CLIENT_SECRET must be set" in str(err.value)


@pytest.mark.parametrize("login_required", [True, False])
@pytest.mark.parametrize("stdin_isatty", [True, False])
@pytest.mark.parametrize("stdin_closed", [True, False])
@pytest.mark.parametrize("is_jupyter", [True, False])
def test_user_app_login_requires_stdin(
    mocker: MockerFixture,
    login_required: bool,
    stdin_isatty: bool,
    stdin_closed: bool,
    is_jupyter: bool,
):
    mock_login_required = mocker.patch.object(GlobusApp, "login_required")
    mock_is_jupyter = mocker.patch(f"{_MOCK_BASE}_is_jupyter")
    mock_stdin = mocker.patch(f"{_MOCK_BASE}sys.stdin")

    mock_login_required.return_value = login_required
    mock_stdin.closed = stdin_closed
    mock_stdin.isatty.return_value = stdin_isatty
    mock_is_jupyter.return_value = is_jupyter

    if login_required and (not stdin_isatty or stdin_closed) and not is_jupyter:
        with pytest.raises(RuntimeError) as err:
            get_globus_app()
        assert "stdin is closed" in err.value.args[0]
        assert "is not a TTY" in err.value.args[0]
        assert "native app" in err.value.args[0]
    else:
        app = get_globus_app()
        assert isinstance(app, UserApp)
