from __future__ import annotations

import os as _os
import pwd as _pwd
import re as _re
import sys
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

    # list of targeted "you can change user" CAPs.  Some of the CAPs in
    # _MULTI_USER_CAPS might be given legitimately to a process (e.g.,
    # BPF, NET_BIND_SERVICE); just in case, separate out the ones that are
    # very, very likely of interest for changing the UID
    _USER_CHANGE_CAPS = {
        _pyprctl.Cap.CHOWN,
        _pyprctl.Cap.DAC_OVERRIDE,
        _pyprctl.Cap.DAC_READ_SEARCH,
        _pyprctl.Cap.FOWNER,
        _pyprctl.Cap.FSETID,
        _pyprctl.Cap.SETGID,
        _pyprctl.Cap.SETFCAP,
        _pyprctl.Cap.SETUID,
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


def is_privileged(posix_user=None, user_privs_only=False) -> bool:
    if not posix_user:
        posix_user = _pwd.getpwuid(_os.getuid())

    caps_to_check = user_privs_only and _USER_CHANGE_CAPS or _MULTI_USER_CAPS
    proc_caps = _pyprctl.CapState.get_current()
    has_privileges = posix_user.pw_uid == 0
    has_privileges |= posix_user.pw_name == "root"
    has_privileges |= any(c in proc_caps.effective for c in caps_to_check)
    return has_privileges


def update_url_port(url_string: str, new_port: int | str) -> str:
    c_url = urllib.parse.urlparse(url_string)
    if c_url.port:
        netloc = c_url.netloc.replace(f":{c_url.port}", f":{new_port}")
    else:
        netloc = c_url.netloc + f":{new_port}"
    c_url = c_url._replace(netloc=netloc)
    return urllib.parse.urlunparse(c_url)


def send_endpoint_startup_failure_to_amqp(amqp_creds: dict, msg: str | None = None):
    """
    Does not handle any exceptions.

    Non-exhaustive possible exceptions:
      - ``ImportError`` - for example, expects ``pika``
      - ``KeyError`` - If the ``amqp_creds`` data structure does not match; see, for
           example, ``endpoint_manager.py`` for the expected data structure.
      - pika connection errors, if unable to open a connection or send a message
    """
    import pika
    from globus_compute_common import messagepack
    from globus_compute_common.messagepack.message_types import EPStatusReport

    if msg is None:
        msg = "General or unknown failure starting user endpoint"

    result_q_info = amqp_creds["result_queue_info"]
    publish_kw = result_q_info["queue_publish_kwargs"]
    urlp = pika.URLParameters(result_q_info["connection_url"])
    status_report = EPStatusReport(
        endpoint_id=amqp_creds["endpoint_id"],
        global_state={"error": msg, "heartbeat_period": 3},
        task_statuses={},
    )
    payload = messagepack.pack(status_report)
    with pika.BlockingConnection(urlp) as mq_conn:
        with mq_conn.channel() as mq_chan:
            mq_chan.basic_publish(
                exchange=publish_kw["exchange"],
                routing_key=publish_kw["routing_key"],
                body=payload,
                mandatory=True,
            )


def user_input_select(prompt: str, options: list[str]) -> str | None:
    """
    Prompt the user to select from a list of options, and returns the item or
    None if the user declines to select one
    """
    assert options, "Options list must not be empty"

    if not (sys.stdin and not sys.stdin.closed):
        raise EOFError("No input (stdin closed); cannot collect user input")

    if not sys.stdin.isatty():
        print("Reading batch data from stdin ...", file=sys.stderr)

    print(prompt)
    for idx, option_text in enumerate(options, start=1):
        print(f"  [{idx}]: {option_text}")

    while True:
        input_raw = input("\nChoice: ")
        if not input_raw.strip():
            return None
        else:
            if input_raw.isdigit():
                input_num = int(input_raw)
                if 0 < input_num <= len(options):
                    return options[input_num - 1]
            print(f"Invalid choice: {input_raw}")
