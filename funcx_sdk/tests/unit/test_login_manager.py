import os
import uuid
from unittest import mock

import globus_sdk
import pytest

from funcx.sdk._environments import _get_envname
from funcx.sdk.login_manager.client_login import (
    _get_client_creds_from_env,
    get_client_login,
    is_client_login,
)
from funcx.sdk.login_manager.manager import LoginManager
from funcx.sdk.login_manager.tokenstore import _resolve_namespace

CID_KEY = "FUNCX_SDK_CLIENT_ID"
CSC_KEY = "FUNCX_SDK_CLIENT_SECRET"
MOCK_BASE = "funcx.sdk.login_manager"


@pytest.fixture
def logman(mocker, tmp_path):
    home = mocker.patch(f"{MOCK_BASE}.tokenstore._home")
    home.return_value = tmp_path
    return LoginManager()


def test_get_client_creds_from_env(randomstring):
    for expected_cid, expected_csc in (
        (randomstring(), randomstring()),
        ("", None),
        (None, ""),
        (None, None),
    ):
        env = {}
        if expected_cid is not None:
            env[CID_KEY] = expected_cid
        if expected_csc is not None:
            env[CSC_KEY] = expected_csc
        with mock.patch.dict(os.environ, env):
            found_cid, found_csc = _get_client_creds_from_env()

        assert expected_cid == found_cid
        assert expected_csc == found_csc


def test_is_client_login():
    env = {CID_KEY: "some_id", CSC_KEY: "some_secret"}
    with mock.patch.dict(os.environ, env):
        assert is_client_login()

    for cid, csc in (("", ""), ("", None), (None, ""), (None, None)):
        env = {}
        if cid is not None:
            env[CID_KEY] = cid
        if csc is not None:
            env[CSC_KEY] = csc
        with mock.patch.dict(os.environ, env):
            assert not is_client_login()

    for cid, csc in (
        ("some_id", ""),
        ("some_id", None),
        ("", "some_secret"),
        (None, "some_secret"),
    ):
        env = {}
        if cid is not None:
            env[CID_KEY] = cid
        if csc is not None:
            env[CSC_KEY] = csc
        with mock.patch.dict(os.environ, env):
            with pytest.raises(ValueError) as err:
                is_client_login()

    assert "Both FUNCX_SDK_CLIENT_ID and FUNCX_SDK_CLIENT_SECRET" in str(err)


def test_get_client_login(caplog, randomstring):
    for cid, csc in (("", ""), ("", None), (None, ""), (None, None)):
        env = {}
        if cid is not None:
            env[CID_KEY] = cid
        if csc is not None:
            env[CSC_KEY] = csc
        with mock.patch.dict(os.environ, env):
            with pytest.raises(ValueError) as err:
                get_client_login()

    assert "No client is logged in" in str(err)

    env = {CID_KEY: str(uuid.uuid4()), CSC_KEY: "some_secret"}
    with mock.patch.dict(os.environ, env):
        rv = get_client_login()

    assert isinstance(rv, globus_sdk.ConfidentialAppAuthClient)
    assert "VERY LIKELY" not in caplog.text

    env = {CID_KEY: randomstring(), CSC_KEY: randomstring()}
    with mock.patch.dict(os.environ, env):
        rv = get_client_login()

    assert isinstance(rv, globus_sdk.ConfidentialAppAuthClient)
    assert "VERY LIKELY INVALID CLIENT ID" in caplog.text
    assert rv.client_id == env[CID_KEY]
    assert rv.authorizer.password == env[CSC_KEY]


def test_resolve_namespace(randomstring):
    client_id = str(uuid.uuid4())
    env = {CID_KEY: client_id, CSC_KEY: randomstring()}

    for ns_env in (randomstring, "", "123", None):
        ns = _resolve_namespace(ns_env)
        ns_env = _get_envname() if ns_env is None else ns_env
        assert ns == f"user/{ns_env}"

        with mock.patch.dict(os.environ, env):
            ns = _resolve_namespace(ns_env)
            assert ns == f"clientprofile/{ns_env}/{client_id}"


def test_link_login_flow_requires_stdin(mocker, logman):
    mocker.patch(f"{MOCK_BASE}.manager.do_link_auth_flow")
    mock_stdin = mocker.patch(f"{MOCK_BASE}.manager.sys.stdin")
    mock_stdin.isatty.return_value = False
    with pytest.raises(RuntimeError) as err:
        logman.run_login_flow()
    assert "stdin is closed" in err.value.args[0]
    assert "is not a TTY" in err.value.args[0]
    assert "native app" in err.value.args[0]

    mock_stdin.isatty.return_value = True
    mock_stdin.closed = False
    logman.run_login_flow()


def test_run_login_flow_ignored_if_client_login(mocker, logman):
    mock_laf = mocker.patch(f"{MOCK_BASE}.manager.do_link_auth_flow")
    mock_stdin = mocker.patch(f"{MOCK_BASE}.manager.sys.stdin")
    mock_stdin.isatty.return_value = True
    mock_stdin.closed = False
    env = {CID_KEY: str(uuid.uuid4()), CSC_KEY: "some_secret"}
    with mock.patch.dict(os.environ, env):
        logman.run_login_flow()
    mock_laf.assert_not_called()

    logman.run_login_flow()
    mock_laf.assert_called()


def test_get_authorizer(mocker, logman):
    mock_gsdk = mocker.patch(f"{MOCK_BASE}.manager.globus_sdk")
    env = {CID_KEY: str(uuid.uuid4()), CSC_KEY: "some_secret"}
    with mock.patch.dict(os.environ, env):
        logman._get_authorizer("some_resource_server")
    mock_gsdk.ClientCredentialsAuthorizer.assert_called()

    with pytest.raises(LookupError):
        logman._get_authorizer("some_resource_server")
