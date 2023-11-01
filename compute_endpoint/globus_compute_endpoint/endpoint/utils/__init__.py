from __future__ import annotations

import os as _os
import pwd as _pwd
import re as _re
import typing as t
import urllib.parse

try:
    import pyprctl as _pyprctl

    has_pyprctl = True
except AttributeError:
    has_pyprctl = False


_url_user_pass_pattern = r"://([^:]+):[^@]+@"
_url_user_pass_re = _re.compile(_url_user_pass_pattern)
_urlb_user_pass_re = _re.compile(_url_user_pass_pattern.encode())

_T = t.TypeVar("_T", str, bytes)

if has_pyprctl:
    # List of Linux capabilities that would suggest the running process is
    # privileged, even if os.getuid() != 0.
    _MULTI_USER_CAPS = {
        _pyprctl.Cap.AUDIT_CONTROL,
        _pyprctl.Cap.AUDIT_READ,
        _pyprctl.Cap.BPF,
        _pyprctl.Cap.CHECKPOINT_RESTORE,
        _pyprctl.Cap.CHOWN,
        _pyprctl.Cap.DAC_OVERRIDE,
        _pyprctl.Cap.DAC_READ_SEARCH,
        _pyprctl.Cap.FOWNER,
        _pyprctl.Cap.FSETID,
        _pyprctl.Cap.IPC_OWNER,
        _pyprctl.Cap.KILL,
        _pyprctl.Cap.LINUX_IMMUTABLE,
        _pyprctl.Cap.MAC_ADMIN,
        _pyprctl.Cap.MAC_OVERRIDE,
        _pyprctl.Cap.MKNOD,
        _pyprctl.Cap.NET_ADMIN,
        _pyprctl.Cap.NET_BIND_SERVICE,
        _pyprctl.Cap.NET_RAW,
        _pyprctl.Cap.PERFMON,
        _pyprctl.Cap.SETGID,
        _pyprctl.Cap.SETFCAP,
        _pyprctl.Cap.SETPCAP,
        _pyprctl.Cap.SETUID,
        _pyprctl.Cap.SYS_ADMIN,
        _pyprctl.Cap.SYS_BOOT,
        _pyprctl.Cap.SYS_CHROOT,
        _pyprctl.Cap.SYS_MODULE,
        _pyprctl.Cap.SYS_NICE,
        _pyprctl.Cap.SYS_PACCT,
        _pyprctl.Cap.SYS_PTRACE,
        _pyprctl.Cap.SYS_RAWIO,
        _pyprctl.Cap.SYS_RESOURCE,
        _pyprctl.Cap.SYS_TIME,
        _pyprctl.Cap.SYS_TTY_CONFIG,
        _pyprctl.Cap.SYSLOG,
        _pyprctl.Cap.WAKE_ALARM,
    }


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


def is_privileged(posix_user=None):
    if not posix_user:
        posix_user = _pwd.getpwuid(_os.getuid())

    proc_caps = _pyprctl.CapState.get_current()
    has_privileges = posix_user.pw_uid == 0
    has_privileges |= posix_user.pw_name == "root"
    has_privileges |= any(c in proc_caps.effective for c in _MULTI_USER_CAPS)
    return has_privileges


def update_url_port(url_string: str, new_port: int | str) -> str:
    c_url = urllib.parse.urlparse(url_string)
    if c_url.port:
        netloc = c_url.netloc.replace(f":{c_url.port}", f":{new_port}")
    else:
        netloc = c_url.netloc + f":{new_port}"
    c_url = c_url._replace(netloc=netloc)
    return urllib.parse.urlunparse(c_url)
