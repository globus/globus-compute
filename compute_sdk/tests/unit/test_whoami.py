import globus_compute_sdk.sdk.login_manager
import pytest
from globus_compute_sdk.sdk.login_manager.whoami import print_whoami_info

MOCK_BASE = "globus_compute_sdk.sdk.login_manager.whoami"


@pytest.mark.parametrize(
    "response_output",
    [
        [
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
    ],
)
def test_whoami(response_output, mocker, monkeypatch):
    linked, resp, profile, has_err, err_msg, num_rows, user_info = response_output

    print_mock = mocker.patch(f"{MOCK_BASE}.print_table")
    oa_mock = mocker.Mock()
    oa_mock.return_value.oauth2_userinfo.return_value = resp
    oa_mock.return_value.get_identities.return_value = profile
    monkeypatch.setattr(
        globus_compute_sdk.sdk.login_manager.LoginManager, "get_auth_client", oa_mock
    )

    if has_err:
        with pytest.raises(ValueError, match=err_msg):
            print_whoami_info(linked)
    else:
        print_whoami_info(linked)
        print_mock.assert_called_once()
        assert num_rows == len(print_mock.call_args[0][1])
        assert user_info == print_mock.call_args[0][1][-1]
