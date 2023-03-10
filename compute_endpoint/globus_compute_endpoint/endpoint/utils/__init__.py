import re as _re
import typing as t

_url_user_pass_pattern = r"://([^:]+):[^@]+@"
_url_user_pass_re = _re.compile(_url_user_pass_pattern)
_urlb_user_pass_re = _re.compile(_url_user_pass_pattern.encode())

_T = t.TypeVar("_T", str, bytes)


def _redact_url_creds(raw: _T, redact_user=True, repl="***", count=0) -> _T:
    """
    Redact URL credentials found in `raw`, by replacing the password and
    (optionally) username with `repl`.

    (A wrapper over `re.sub()`)

    :param raw: The raw string to be redacted.
    :param redact_user: If false, do not redact the username
    :param count: If 0, replace all found occurrences; otherwise, replace only count
    :return: A string (or bytes) with URL credentials redacted
    """
    if redact_user:
        repl = rf"://{repl}:{repl}@"
    else:
        repl = rf"://\1:{repl}@"
    if isinstance(raw, str):
        return _url_user_pass_re.sub(repl=repl, string=raw, count=count)
    return _urlb_user_pass_re.sub(repl=repl.encode(), string=raw, count=count)
