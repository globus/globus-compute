import pytest

from funcx.sdk.login_manager.manager import LoginManager


@pytest.fixture
def logman(mocker, tmp_path):
    home = mocker.patch("funcx.sdk.login_manager.tokenstore._home")
    home.return_value = tmp_path
    return LoginManager()


def test_link_login_flow_requires_stdin(mocker, logman):
    mocker.patch("funcx.sdk.login_manager.manager.do_link_auth_flow")
    mock_stdin = mocker.patch("funcx.sdk.login_manager.manager.sys.stdin")
    mock_stdin.isatty.return_value = False
    with pytest.raises(RuntimeError) as err:
        logman.run_login_flow()
    assert "stdin is closed" in err.value.args[0]
    assert "is not a TTY" in err.value.args[0]
    assert "native app" in err.value.args[0]

    mock_stdin.isatty.return_value = True
    mock_stdin.closed = False
    logman.run_login_flow()
