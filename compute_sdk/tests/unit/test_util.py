import pytest
import requests
import responses
from globus_compute_sdk.sdk.utils import check_version
from globus_compute_sdk.sdk.utils.gare import gare_handler
from globus_sdk import GlobusAPIError, GlobusApp

_GARE_MOCK_BASE = "globus_compute_sdk.sdk.utils.gare."


@pytest.mark.parametrize(
    ("sdk_py", "worker_py", "check_micro", "should_warn"),
    (
        ["3.11.8", "3.10.8", False, True],
        ["3.11.8", "3.10.8", True, True],
        ["3.11.8", "3.11.7", False, False],
        ["3.11.8", "3.11.7", True, True],
        ["3.11.8", None, False, True],
        ["3.11.8", None, True, True],
    ),
)
def test_check_py_version(mocker, sdk_py, worker_py, check_micro, should_warn):
    mock_details = mocker.patch("globus_compute_sdk.sdk.utils.get_env_details")
    mock_details.return_value = {
        "os": "some-64bit",
        "dill_version": "0.3.5.1",
        "python_version": sdk_py,
        "globus_compute_sdk_version": "2.22.0",
    }
    task_details = {
        "os": "Linux-5.19.0-1025-aws-x86_64-with-glibc2.35",
        "python_version": worker_py,
        "dill_version": "0.3.5.1",
        "globus_compute_sdk_version": "2.3.2",
        "task_transitions": {
            "execution-start": 1692742841.843334,
            "execution-end": 1692742846.123456,
        },
    }
    if worker_py is None:
        del task_details["python_version"]

    result = check_version(task_details, check_py_micro=check_micro)
    if should_warn:
        assert result and "Environment differences detected" in result
    else:
        assert result is None


@pytest.fixture
def mock_erroring_api():
    responses.add(
        responses.GET,
        "https://api.globus.org/some/endpoint",
        json={"code": 100, "message": "Some error"},
        status=500,
    )

    def error_api():
        response = requests.get("https://api.globus.org/some/endpoint")
        raise GlobusAPIError(response)

    responses.start()

    yield error_api

    responses.stop()
    responses.reset()


def test_gare_handler_success(mocker):
    mock_app = mocker.Mock(spec=GlobusApp)

    def personal_sum(x, y, *, requested_by):
        return f"{x + y} for {requested_by}"

    result = gare_handler(mock_app.login, personal_sum, 1, 2, requested_by="Kevin")

    assert result == "3 for Kevin"


def test_gare_handler_globus_api_error_with_gares(mocker, mock_erroring_api):
    mock_app = mocker.Mock(spec=GlobusApp)
    mock_gares = [mocker.Mock(), mocker.Mock()]
    mocker.patch(f"{_GARE_MOCK_BASE}to_gares", return_value=mock_gares)

    with pytest.raises(GlobusAPIError):
        gare_handler(mock_app.login, mock_erroring_api)

    for gare in mock_gares:
        mock_app.login.assert_any_call(auth_params=gare.authorization_parameters)


def test_gare_handler_globus_api_error_no_gares(mocker, mock_erroring_api):
    mock_app = mocker.Mock(spec=GlobusApp)
    mocker.patch(f"{_GARE_MOCK_BASE}to_gares", return_value=[])

    with pytest.raises(GlobusAPIError):
        gare_handler(mock_app.login, mock_erroring_api)

    mock_app.login.assert_not_called()


def test_gare_handler_sets_session_message(mocker, mock_erroring_api):
    mock_app = mocker.Mock(spec=GlobusApp)
    mock_gare = mocker.Mock()
    mock_gare.authorization_parameters.session_message = None
    mock_gare.extra = {"reason": "Some reason"}
    mocker.patch(f"{_GARE_MOCK_BASE}to_gares", return_value=[mock_gare])

    with pytest.raises(GlobusAPIError):
        gare_handler(mock_app.login, mock_erroring_api)

    assert mock_gare.authorization_parameters.session_message == "Some reason"
