from globus_compute_endpoint.endpoint.utils import _redact_url_creds


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
