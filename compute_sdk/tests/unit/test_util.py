import pytest
from globus_compute_sdk.sdk.utils import check_version


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
