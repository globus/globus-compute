from __future__ import annotations

import fcntl as _fcntl
import json
import os as _os
import pwd as _pwd
import re as _re
import resource as _resource
import sys
import time
import typing as t
import urllib.parse

has_pyprctl: bool
pyprctl_import_error: Exception | None

try:
    import pyprctl as _pyprctl
except Exception as e:
    has_pyprctl = False
    pyprctl_import_error = e
else:
    has_pyprctl = True
    pyprctl_import_error = None


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


if not hasattr(_os, "memfd_create"):
    # If `os.memfd_create` does not exist, then either this is running on a *really*
    # old Linux kernel (< 3.17) or the glibc wrapper available during the Python build
    # was too old (glibc < 2.27).  Under the assumption that ~no one is using so out-
    # -of-date a kernel, implement memfd_create manually.  This implementation simply
    # makes the request directly to the kernel (via a syscall), rather than
    # outsourcing to glibc.

    def _create_memfd_shim():
        import ctypes
        import errno

        arch = _os.uname().machine.lower()
        try:
            _MEMFD_CREATE_SYSCALL_ID = {
                # - architecture specific syscall codes, as found in the kernel sources
                #   ... links given inline
                # - if we need to support additional architectures on *really old
                #   distros*, then augment this data structure accordingly.
                # - the keys of this structure are my best guess at what the local libc
                #   uname() returns on different platforms.  Let's wait for bug reports
                #   before trying to augment this too fully.
                #
                # https://github.com/torvalds/linux/blob/v7.2/include/uapi/asm-generic/unistd.h#L675
                "aarch64": 279,
                "arm64": 279,
                # https://github.com/torvalds/linux/blob/v7.2/arch/x86/entry/syscalls/syscall_64.tbl#L331
                "amd64": 319,
                "x86_64": 319,
                # https://github.com/torvalds/linux/blob/v7.2/arch/x86/entry/syscalls/syscall_32.tbl#L371
                "i386": 356,
                "i686": 356,
                # https://github.com/torvalds/linux/blob/v7.2/arch/arm/tools/syscall.tbl#L403
                "armv7": 385,
                "armv7l": 385,
                # https://github.com/torvalds/linux/blob/v7.2/arch/s390/kernel/syscalls/syscall.tbl#L304
                "s390x": 350,
                # https://github.com/torvalds/linux/blob/v7.2/arch/powerpc/kernel/syscalls/syscall.tbl#L464
                "ppc64le": 360,
            }[arch]
        except KeyError as e:
            raise OSError(
                errno.ENOSYS,
                f"Compute's memfd_create() shim: architecture undefined for {arch!r}",
            ) from e

        for mfd_const, val in (
            ("MFD_CLOEXEC", 1),
            ("MFD_ALLOW_SEALING", 2),
            ("MFD_HUGETLB", 4),
            ("MFD_HUGE_SHIFT", 26),
            ("MFD_HUGE_MASK", 63),
            ("MFD_HUGE_64KB", 1073741824),
            ("MFD_HUGE_512KB", 1275068416),
            ("MFD_HUGE_1MB", 1342177280),
            ("MFD_HUGE_2MB", 1409286144),
            ("MFD_HUGE_8MB", 1543503872),
            ("MFD_HUGE_16MB", 1610612736),
            ("MFD_HUGE_32MB", 1677721600),
            ("MFD_HUGE_256MB", 1879048192),
            ("MFD_HUGE_512MB", 1946157056),
            ("MFD_HUGE_1GB", 2013265920),
            ("MFD_HUGE_2GB", 2080374784),
            ("MFD_HUGE_16GB", 2281701376),
        ):
            if not hasattr(_os, mfd_const):
                setattr(_os, mfd_const, val)

        libc = ctypes.CDLL(None, use_errno=True)
        libc.syscall.argtypes = [ctypes.c_long, ctypes.c_char_p, ctypes.c_uint]
        libc.syscall.restype = ctypes.c_long

        def _memfd_create_shim(name: str, flags: int = _os.MFD_CLOEXEC) -> int:
            name_b = _os.fsencode(name)
            fd = libc.syscall(_MEMFD_CREATE_SYSCALL_ID, name_b, flags)
            if fd == -1:
                err_num = ctypes.get_errno()
                raise OSError(err_num, _os.strerror(err_num))
            return int(fd)  # back to Python's integer from a c_long

        _os.memfd_create = _memfd_create_shim

    _create_memfd_shim()


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


def make_close_on_exec(fd: int) -> None:
    flags = _fcntl.fcntl(fd, _fcntl.F_GETFD)
    _fcntl.fcntl(fd, _fcntl.F_SETFD, flags | _fcntl.FD_CLOEXEC)


def close_all_fds(preserve_fds: t.Iterable[int] = ()) -> None:
    _soft_no, hard_no = _resource.getrlimit(_resource.RLIMIT_NOFILE)
    fd_low = 0

    for preserve_fd in sorted(preserve_fds):
        if fd_low < preserve_fd:
            _os.closerange(fd_low, preserve_fd)
        fd_low = preserve_fd + 1
    _os.closerange(fd_low, hard_no + 1)


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

    q_info = amqp_creds["heartbeat_queue_info"]
    publish_kw = q_info["queue_publish_kwargs"]
    urlp = pika.URLParameters(q_info["connection_url"])
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


def make_credential_provider(
    cred_fd: int | None, key: str | bytes | None, reg_info: dict | None = None
) -> t.Callable[[], dict[str, dict]]:
    """
    Dynamically query a credential source.

    The returned data structure is from the Endpoint registration API call,
    encapsulated in the SDK by `Client.register_endpoint()`.  It currently contains
    connection information for the task, result, and heartbeat queues.
    """
    # The non-"else" branches are only required so long as we support pre-"credential
    # refreshing" CEPs.  The clock starts ticking from Sep, 2026.
    if cred_fd is None:
        if reg_info is None:
            raise KeyError("No credential info provided")

        amqp_creds: dict = {"amqp_creds": reg_info}
        del cred_fd, key

        def _credential_provider() -> dict:
            return amqp_creds

    elif not key:
        raise ValueError("Missing required encryption key")

    else:
        from cryptography.fernet import Fernet

        _cred_fd = int(cred_fd)  # damnit mypy, we just proved it!
        del reg_info, cred_fd

        def _credential_provider() -> dict:
            try_count = 1
            while True:
                try:
                    _os.lseek(_cred_fd, 0, _os.SEEK_SET)
                    dyn_data_b = b""
                    while chunk := _os.read(_cred_fd, 2**15):
                        dyn_data_b += chunk

                    enc = Fernet(key)
                    creds = json.loads(enc.decrypt(dyn_data_b))
                    if not creds:
                        raise ValueError("Credentials empty or not written")
                    return creds
                except Exception as exc:
                    if try_count < 1:
                        raise
                    # Maybe we got super unlucky in catching the file mid-truncate
                    # or write; try one more time after small delay
                    try_count -= 1
                    exc_type = type(exc).__name__
                    msg = f"Failed to collect credentials: ({exc_type}) {exc}"
                    print(msg, file=sys.stderr, flush=True)
                    time.sleep(1)

    return _credential_provider
