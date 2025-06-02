import sys

import pytest
from click import ClickException
from globus_compute_endpoint.auth import get_globus_app_with_scopes
from globus_compute_sdk.sdk.auth.auth_client import ComputeAuthClient
from globus_sdk import ComputeClient
from pytest_mock import MockFixture

_MOCK_BASE = "globus_compute_endpoint.auth."


def test_get_globus_app_with_scopes(mocker: MockFixture):
    mock_stdin = mocker.patch.object(sys, "stdin")
    mock_stdin.isatty.return_value = True
    mock_stdin.closed = False

    app = get_globus_app_with_scopes()

    scopes = []
    for scope_list in app.scope_requirements.values():
        for scope in scope_list:
            scopes.append(str(scope))

    assert len(scopes) > 0
    assert all(str(s) in scopes for s in ComputeClient.default_scope_requirements)
    assert all(str(s) in scopes for s in ComputeAuthClient.default_scope_requirements)


@pytest.mark.parametrize("exception_type", [ValueError, RuntimeError])
def test_get_globus_app_with_scopes_handles_exception(
    exception_type: Exception, mocker: MockFixture
):
    mocker.patch(
        f"{_MOCK_BASE}get_globus_app", side_effect=exception_type("Test exception")
    )
    with pytest.raises(ClickException) as exc:
        get_globus_app_with_scopes()
    assert "Test exception" in str(exc)
