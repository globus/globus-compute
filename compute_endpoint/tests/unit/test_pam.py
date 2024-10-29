import random
from unittest import mock

import pytest
from globus_compute_endpoint import pam
from globus_compute_endpoint.pam import PamCred, PamError, PamHandle, PamReturnEnum

_MOCK_BASE = "globus_compute_endpoint.pam."


@pytest.fixture
def pamh():
    return PamHandle("some service", "some username")


def test_pamerror_includes_enum_name(pamh):
    with pamh:
        for errenum in PamReturnEnum:
            e = PamError(pamh, errenum.value)
            assert f"[{errenum.name}]" in str(e), (errenum, "Expect enum name in error")


@pytest.mark.parametrize("name_len", (None, -5, 0, 11, 513))
def test_pamh_username_limits_length(randomstring, name_len):
    if name_len is None:
        pamh = PamHandle("some service")
        assert pamh.max_username_len == 512, "Expect sensible default; match sshd"
    else:
        pamh = PamHandle("some service", max_username_len=name_len)

    assert pamh.max_username_len > 0

    pamh.username = randomstring(pamh.max_username_len + 1)
    assert (
        len(pamh.username) == pamh.max_username_len
    ), "Like ssh, protect buggy PAM implementations from themselves"


def test_pamh_start_does_not_clobber_existing_session(pamh):
    pamh._started = True
    with pytest.raises(RuntimeError):
        pamh.pam_start(pamh.service_name, pamh.username, None)


def test_pamh_start_raises_on_non_success(pamh):
    errenum: PamReturnEnum = random.choice(tuple(PamReturnEnum)[1:])  # no SUCCESS!
    with mock.patch(f"{_MOCK_BASE}_pam_start") as mock_start:
        mock_start.return_value = errenum.value
        with pytest.raises(PamError):
            pamh.pam_start(pamh.service_name, pamh.username, None)
    assert not pamh._started


def test_pamh_start_success(pamh):
    with mock.patch(f"{_MOCK_BASE}_pam_start") as mock_start:
        mock_start.return_value = 0
        pamh.pam_start(pamh.service_name, pamh.username, None)

    a, _k = mock_start.call_args
    assert isinstance(a[0], bytes), "Expect Python str encoded to *bytes*"
    assert isinstance(a[1], bytes), "Expect Python str encoded to *bytes*"
    assert pamh._started, "Expected to mark session is open"


def test_pamh_context_verifies_service_name(pamh):
    pamh.service_name = ""
    with pytest.raises(ValueError) as pyt_e:
        with pamh:
            pass
    e_str = str(pyt_e.value)
    assert "Missing service name" in e_str, "Expect human description of problem"
    assert "use `.service_name`" in e_str, "Expect suggestion in message"
    assert "__init__" in e_str, "Expect suggestion in message"


def test_pamh_context_verifies_username(pamh):
    pamh.username = ""
    with pytest.raises(ValueError) as pyt_e:
        with pamh:
            pass
    e_str = str(pyt_e.value)
    assert "Missing user name" in e_str, "Expect human description of problem"
    assert "use `.username`" in e_str, "Expect suggestion in message"
    assert "__init__" in e_str, "Expect suggestion in message"


def test_pamh_context_starts_session(pamh):
    with mock.patch.object(pamh, "pam_start") as mock_start:
        with pamh as test_pamh:
            assert pamh is test_pamh

    assert mock_start.called
    a, _k = mock_start.call_args
    assert (a[0], a[1]) == (pamh.service_name, pamh.username), "Expect useful defaults"


def test_pamh_context_ends_session(pamh):
    with mock.patch.object(pamh, "pam_end") as mock_end:
        with pamh:
            assert pamh._started
    assert mock_end.called


def test_pamh_context(pamh):
    assert pamh._started is False
    with pamh:
        assert pamh._started
    assert pamh._started is False


def test_pamh_flag_only_methods_raises(pamh):
    errenum: PamReturnEnum = random.choice(tuple(PamReturnEnum)[1:])  # no SUCCESS!

    def return_nonzero(*a, **k):
        return errenum.value

    with pytest.raises(PamError) as pyt_e:
        pamh._flags_only_method(return_nonzero)

    assert pamh.last_rc == errenum.value
    assert errenum.name in str(pyt_e.value)


@pytest.mark.parametrize(
    "fn_name", ("setcred", "acct_mgmt", "open_session", "close_session", "authenticate")
)
def test_pamh_flags_only_passthrough(pamh, fn_name):
    pamh_fn_name = f"pam_{fn_name}"
    pam_lib_fn = getattr(pam, f"_{pamh_fn_name}")
    flags = random.randint(0, 0xFFFF)
    with mock.patch.object(pamh, "_flags_only_method") as mock_fl:
        getattr(pamh, pamh_fn_name)(flags)

    assert mock_fl.called
    a, _k = mock_fl.call_args
    assert a[0] is pam_lib_fn, "Correct PAM callable supplied"
    assert a[1] == flags


@pytest.mark.parametrize(
    "fn_name,flag",
    (
        ("credentials_establish", PamCred.PAM_ESTABLISH_CRED),
        ("credentials_delete", PamCred.PAM_DELETE_CRED),
        ("credentials_refresh", PamCred.PAM_REFRESH_CRED),
        ("credentials_reinitialize", PamCred.PAM_REINITIALIZE_CRED),
    ),
)
def test_pamh_creds(pamh, fn_name, flag):
    with mock.patch.object(pamh, "pam_setcred") as mock_set:
        getattr(pamh, fn_name)()
    assert mock_set.called, "Convenience method wrapper calls pam_setcred"
    a, _k = mock_set.call_args
    assert a[0] is flag
