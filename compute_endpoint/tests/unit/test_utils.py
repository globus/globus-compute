from collections import namedtuple

import pytest
from globus_compute_endpoint.endpoint.utils import (
    _redact_url_creds,
    is_privileged,
    update_url_port,
)

try:
    import pyprctl  # noqa

    _has_pyprctl = True
except AttributeError:
    _has_pyprctl = False


_MOCK_BASE = "globus_compute_endpoint.endpoint.utils."


def test_url_redaction(randomstring):
    scheme = randomstring()
    uname = randomstring()
    pword = randomstring()
    fqdn = randomstring()
    somepath = randomstring()
    some_url = f"{scheme}://{uname}:{pword}@{fqdn}/{somepath}"
    for redact_user in (True, False):
        kwargs = {"redact_user": redact_user}
        for repl in (None, "XxX", "*", "---"):
            if repl:
                kwargs["repl"] = repl
            else:
                repl = "***"  # default replacement

            if redact_user:
                expected = f"{scheme}://{repl}:{repl}@{fqdn}/{somepath}"
            else:
                expected = f"{scheme}://{uname}:{repl}@{fqdn}/{somepath}"
            assert _redact_url_creds(some_url, **kwargs) == expected


@pytest.mark.parametrize("uid", (0, 1000))
def test_is_privileged_tests_against_uid(mocker, uid):
    if not _has_pyprctl:
        pytest.skip()

    user = namedtuple("posix_user", "pw_uid,pw_name")(uid, "asdf")
    mock_prctl = mocker.patch(f"{_MOCK_BASE}_pyprctl")
    mock_prctl.CapState.get_current.return_value.effective = {}

    assert is_privileged(user) is bool(0 == uid)


@pytest.mark.parametrize("uname", ("root", "not_root_uname"))
def test_is_privileged_tests_for_root_username(mocker, uname):
    if not _has_pyprctl:
        pytest.skip()

    user = namedtuple("posix_user", "pw_uid,pw_name")(987, uname)
    mock_prctl = mocker.patch(f"{_MOCK_BASE}_pyprctl")
    mock_prctl.CapState.get_current.return_value.effective = {}

    assert is_privileged(user) is bool("root" == uname)


if _has_pyprctl:

    @pytest.mark.parametrize("cap", ({pyprctl.Cap.SYS_ADMIN}, {}))
    def test_is_privileged_checks_for_privileges(mocker, cap):
        if not _has_pyprctl:
            pytest.skip()

        user = namedtuple("posix_user", "pw_uid,pw_name")(987, "asdf")
        mock_prctl = mocker.patch(f"{_MOCK_BASE}_pyprctl")
        mock_prctl.CapState.get_current.return_value.effective = cap
        assert is_privileged(user) is bool(cap)


@pytest.mark.parametrize(
    "start_url, port, end_url",
    [
        ("amqp://some.domain:1234", 1111, "amqp://some.domain:1111"),
        ("https://domain.com:4567/homepage", 2222, "https://domain.com:2222/homepage"),
        (
            "postgres://user:pass@some.domain:5678",
            3333,
            "postgres://user:pass@some.domain:3333",
        ),
        (
            "postgres://user:pass@some.domain/funcx",
            4444,
            "postgres://user:pass@some.domain:4444/funcx",
        ),
    ],
)
def test_update_url_port(start_url, port, end_url):
    assert update_url_port(start_url, port) == end_url
