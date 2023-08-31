from itertools import chain, combinations

import pytest
from globus_compute_sdk.sdk.login_manager import AuthorizerLoginManager
from globus_compute_sdk.sdk.login_manager.manager import LoginManager

CID_KEY = "FUNCX_SDK_CLIENT_ID"
CSC_KEY = "FUNCX_SDK_CLIENT_SECRET"
MOCK_BASE = "globus_compute_sdk.sdk.login_manager"


@pytest.fixture
def logman(mocker, tmp_path):
    home = mocker.patch(f"{MOCK_BASE}.tokenstore._home")
    home.return_value = tmp_path
    return AuthorizerLoginManager({})


@pytest.mark.parametrize(
    "missing_keys",
    list(
        chain(
            combinations(LoginManager.SCOPES, 1),
            combinations(LoginManager.SCOPES, 2),
            [()],
        )
    ),
)
def test_ensure_logged_in(mocker, logman, missing_keys):
    _authorizers = dict(LoginManager.SCOPES)
    for k in missing_keys:
        _authorizers.pop(k, None)

    logman.authorizers = _authorizers

    if missing_keys:
        with pytest.raises(LookupError) as err:
            logman.ensure_logged_in()

        assert f"could not find authorizer for {missing_keys[0]}" in err.value.args[0]


def test_warns_upon_logout_attempt(mocker):
    mock_log = mocker.patch(f"{MOCK_BASE}.authorizer_login_manager.log")
    alm = AuthorizerLoginManager(mocker.Mock())
    alm.logout()
    assert mock_log.warning.called
