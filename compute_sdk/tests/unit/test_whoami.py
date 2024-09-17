import pytest
from globus_compute_sdk.sdk.auth.auth_client import ComputeAuthClient
from globus_compute_sdk.sdk.auth.whoami import NOT_LOGGED_IN_MSG, print_whoami_info
from globus_sdk.experimental.globus_app import UserApp
from pytest_mock import MockerFixture

MOCK_BASE = "globus_compute_sdk.sdk.auth.whoami"


@pytest.mark.parametrize(
    "response_output",
    [
        [
            True,
            False,
            {
                "sub": "id_abc",
                "last_authentication": 1674588197,
                "identity_set": [{"sub": "id_def", "last_authentication": 1674588197}],
            },
            {
                "identities": [
                    {
                        "id": "id_abc",
                        "username": "abc@example.com",
                        "name": "first last",
                        "email": "abc@example.com",
                    },
                ]
            },
            False,
            "",
            1,
            ["abc@example.com", "first last", "id_abc", "abc@example.com"],
        ],
        [
            True,
            True,
            {
                "sub": "id_abc",
                "last_authentication": 1674588197,
                "identity_set": [
                    {"sub": "id_abc", "last_authentication": 1674588197},
                    {"sub": "id_def", "last_authentication": 1674588197},
                ],
            },
            {
                "identities": [
                    {
                        "id": "id_def",
                        "username": "def@example.com",
                        "name": "first last",
                        "email": "def@example.com",
                    },
                ]
            },
            False,
            "",
            2,
            ["def@example.com", "first last", "id_def", "def@example.com"],
        ],
        [
            True,
            True,
            {
                "sub": "abc",
                "last_authentication": 1674588197,
            },
            {
                "identities": [
                    {
                        "id": "id_abc",
                        "username": "abc@example.com",
                        "name": "first last",
                        "email": "abc@example.com",
                    }
                ]
            },
            True,
            "full identity set",
            0,
            [],
        ],
        [
            False,
            False,
            {},
            {},
            True,
            NOT_LOGGED_IN_MSG,
            0,
            [],
        ],
    ],
)
def test_whoami(response_output, mocker: MockerFixture):
    logged_in, linked, resp, profile, has_err, err_msg, num_rows, user_info = (
        response_output
    )

    print_mock = mocker.patch(f"{MOCK_BASE}.print_table")
    mock_auth_client = mocker.Mock(spec=ComputeAuthClient)
    mock_auth_client.oauth2_userinfo.return_value = resp
    mock_auth_client.get_identities.return_value = profile
    mocker.patch(f"{MOCK_BASE}.ComputeAuthClient", return_value=mock_auth_client)
    mock_app = mocker.Mock(spec=UserApp)
    mock_app.login_required.return_value = not logged_in

    if has_err:
        with pytest.raises(ValueError, match=err_msg):
            print_whoami_info(mock_app, linked)
    else:
        print_whoami_info(mock_app, linked)
        print_mock.assert_called_once()
        assert num_rows == len(print_mock.call_args[0][1])
        assert user_info == print_mock.call_args[0][1][-1]
