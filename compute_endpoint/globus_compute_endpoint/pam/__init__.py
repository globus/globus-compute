from __future__ import annotations

import typing as t
from ctypes import CDLL, CFUNCTYPE, POINTER, Structure, byref, c_char_p, c_int, c_void_p
from ctypes.util import find_library
from enum import Enum

pam_lib_name = find_library("pam")
if pam_lib_name is None:
    msg = (
        "Failed to find `pam` shared library\n"
        "\n  Hints:"
        "\n    - install libpam.so? (via yum, apt, etc.)"
        "\n    - ldconfig -p"
        "\n    - LD_PRELOAD"
    )
    raise ImportError(msg)

libpam = CDLL(pam_lib_name)


# Constants (interpreted here as Enums) as defined in libpam; see _pam_types.h
PAM_SUCCESS = 0


class PamReturnEnum(Enum):
    PAM_SUCCESS = PAM_SUCCESS

    PAM_OPEN_ERR = 1  # dlopen() failure when dynamically loading a service module
    PAM_SYMBOL_ERR = 2
    PAM_SERVICE_ERR = 3
    PAM_SYSTEM_ERR = 4
    PAM_BUF_ERR = 5
    PAM_PERM_DENIED = 6
    PAM_AUTH_ERR = 7
    PAM_CRED_INSUFFICIENT = 8
    PAM_AUTHINFO_UNAVAIL = 9
    PAM_USER_UNKNOWN = 10
    PAM_MAXTRIES = 11
    PAM_NEW_AUTHTOK_REQD = 12
    PAM_ACCT_EXPIRED = 13
    PAM_SESSION_ERR = 14
    PAM_CRED_UNAVAIL = 15
    PAM_CRED_EXPIRED = 16
    PAM_CRED_ERR = 17
    PAM_NO_MODULE_DATA = 18
    PAM_CONV_ERR = 19
    PAM_AUTHTOK_ERR = 20
    PAM_AUTHTOK_RECOVERY_ERR = 21
    PAM_AUTHTOK_LOCK_BUSY = 22
    PAM_AUTHTOK_DISABLE_AGING = 23
    PAM_TRY_AGAIN = 24
    PAM_IGNORE = 25
    PAM_ABORT = 26
    PAM_AUTHTOK_EXPIRED = 27
    PAM_MODULE_UNKNOWN = 28
    PAM_BAD_ITEM = 29
    PAM_CONV_AGAIN = 30
    PAM_INCOMPLETE = 31


class PamPrompt(Enum):
    # message styles
    PAM_PROMPT_ECHO_OFF = 1
    PAM_PROMPT_ECHO_ON = 2
    PAM_ERROR_MSG = 3
    PAM_TEXT_INFO = 4


class PamAuthenticate(Enum):
    PAM_DISALLOW_NULL_AUTHTOK = 0x1
    PAM_SILENT = 0x8000


class PamCred(Enum):
    PAM_ESTABLISH_CRED = 0x2
    PAM_DELETE_CRED = 0x4
    PAM_REINITIALIZE_CRED = 0x8
    PAM_REFRESH_CRED = 0x10
    PAM_SILENT = 0x8000


class PamAcctMgmt(Enum):
    PAM_DISALLOW_NULL_AUTHTOK = 0x1
    PAM_SILENT = 0x8000


class PamSession(Enum):
    PAM_SILENT = 0x8000


class PamError(RuntimeError):
    def __init__(
        self,
        pam_handle: PamHandle,
        pam_error_code: int,
    ):
        msg = _pam_strerror(pam_handle, pam_error_code).decode()
        try:
            const_name = f" [{PamReturnEnum(pam_error_code).name}]"
        except ValueError:
            const_name = ""
        super().__init__(f"(error code: {pam_error_code}{const_name}) {msg}")


class PamMessage(Structure):
    """Wrapper for `pam_message` struct; see _pam_types.h"""

    _fields_ = (("msg_style", c_int), ("msg", c_char_p))

    def __repr__(self) -> str:
        c_name = type(self).__name__
        return f"{c_name}({self.msg_style}, {self.msg!r})"


class PamResponse(Structure):
    """Wrapper for `pam_response` struct; see _pam_types.h"""

    _fields_ = (("resp", c_char_p), ("resp_retcode", c_int))

    def __repr__(self) -> str:
        c_name = type(self).__name__
        # emit the `resp_retcode` for completeness, but as of this implementation
        # note that the C library does not use `resp_retcode`
        return f"{c_name}({self.resp!r}, {self.resp_retcode})"


_conv_fn = CFUNCTYPE(
    c_int, c_int, POINTER(POINTER(PamMessage)), POINTER(POINTER(PamResponse)), c_void_p
)


@_conv_fn
def _noop(*a):
    return 0


class PamConversation(Structure):
    """Wrapper for `pam_conv` struct; see _pam_types.h"""

    _fields_ = (("conv", _conv_fn), ("appdata_ptr", c_void_p))

    def __repr__(self) -> str:
        # Use angle brackets as this output can't be used to create another instance
        c_name = type(self).__name__
        return f"{c_name}<{self.conv}, {self.appdata_ptr}>"


PamConversationFunctionType = t.Callable[[int, PamMessage, PamResponse, c_void_p], int]


class PamHandle(Structure):
    """Object-oriented wrapper of pam_handle_t"""

    _fields_ = (("handle", c_void_p),)

    def __init__(
        self,
        service_name: str = "",
        username: str = "",
        conversation_fn: PamConversationFunctionType | None = None,
        *,
        max_username_len: int = 512,
    ):
        super().__init__()

        self._started = False
        self.service_name = service_name
        self.username = username
        self.conversation_fn = conversation_fn
        self.last_rc: int = 0
        self.max_username_len = max_username_len

    @property
    def service_name(self) -> str:
        return self._service_name

    @service_name.setter
    def service_name(self, name: str):
        if name is None:
            raise ValueError("service_name must be a string")
        self._service_name = name

    @property
    def max_username_len(self) -> int:
        return self._max_username_len

    @max_username_len.setter
    def max_username_len(self, val: int):
        self._max_username_len = max(1, val)

    @property
    def username(self) -> str:
        # Protect buggy PAM implementations from themselves
        return self._username[: self.max_username_len]

    @username.setter
    def username(self, name: str):
        if name is None:
            raise ValueError("username must be a string")
        self._username = name

    def __enter__(self):
        sname = self.service_name
        uname = self.username
        if not sname:
            raise ValueError("Missing service name; use `.service_name` or __init__ ")
        if not uname:
            raise ValueError("Missing user name; use `.username` or __init__")
        self.pam_start(sname, uname, self.conversation_fn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pam_end()

    def pam_start(
        self,
        service_name: str,
        username: str,
        conversation_fn: (
            t.Callable[[int, PamMessage, PamResponse, c_void_p], int] | None
        ),
    ):
        if self._started:
            raise RuntimeError("Existing session not ended.")
        conversation_fn = conversation_fn or _noop
        pamc = PamConversation(_conv_fn(conversation_fn), 0)
        sname_bytes = service_name.encode()
        username_bytes = username.encode()
        self.last_rc = _pam_start(sname_bytes, username_bytes, byref(pamc), byref(self))
        if self.last_rc != PAM_SUCCESS:
            raise PamError(self, self.last_rc)
        self._started = True

    def pam_end(self):
        if not self._started:
            return
        self.last_rc = _pam_end(self, self.last_rc)
        self._started = False
        if self.last_rc != PAM_SUCCESS:
            raise PamError(self, self.last_rc)

    def _flags_only_method(self, pam_fn: t.Callable, flags: Enum | int = 0):
        """If flags sets more than one bit, pass an int, not an Enum"""
        _fl: int = flags if isinstance(flags, int) else flags.value
        self.last_rc = pam_fn(self, _fl)
        if self.last_rc != PAM_SUCCESS:
            raise PamError(self, self.last_rc)

    def pam_setcred(self, flags: PamCred | int = 0):
        """If flags sets more than one bit, pass an int, not a PamCred"""
        return self._flags_only_method(_pam_setcred, flags)

    def pam_acct_mgmt(self, flags: PamAcctMgmt | int = 0):
        """If flags sets more than one bit, pass an int, not a PamAcctMgmt"""
        return self._flags_only_method(_pam_acct_mgmt, flags)

    def pam_open_session(self, flags: PamSession | int = 0):
        """If flags sets more than one bit, pass an int, not a PamSession"""
        return self._flags_only_method(_pam_open_session, flags)

    def pam_close_session(self, flags: PamSession | int = 0):
        """If flags sets more than one bit, pass an int, not a PamSession"""
        return self._flags_only_method(_pam_close_session, flags)

    def pam_authenticate(self, flags: PamAuthenticate | int = 0):
        """If flags sets more than one bit, pass an int, not a PamSession"""
        return self._flags_only_method(_pam_authenticate, flags)

    def credentials_establish(self):
        return self.pam_setcred(PamCred.PAM_ESTABLISH_CRED)

    def credentials_delete(self):
        return self.pam_setcred(PamCred.PAM_DELETE_CRED)

    def credentials_refresh(self):
        return self.pam_setcred(PamCred.PAM_REFRESH_CRED)

    def credentials_reinitialize(self):
        return self.pam_setcred(PamCred.PAM_REINITIALIZE_CRED)


def _pam_fn_unavailable(fn_name: str, exc: Exception):
    def wrapped(*_a, **_k):
        raise AttributeError(f"{fn_name} not available in loaded PAM library") from exc

    return wrapped


# Not try-except wrapped: required PAM-accessible functions or "just go home."
_pam_start = libpam.pam_start
_pam_start.restype = c_int
_pam_start.argtypes = (c_char_p, c_char_p, POINTER(PamConversation), POINTER(PamHandle))

_pam_end = libpam.pam_end
_pam_end.restype = c_int
_pam_end.argtypes = (PamHandle, c_int)

_pam_strerror = libpam.pam_strerror
_pam_strerror.restype = c_char_p
_pam_strerror.argtypes = (PamHandle, c_int)


# Other PAM-accessible functions may or may not be required for individual applications,
# so only error out *when they're invoked.*  If a function is not exported by the found
# PAM C-library but the calling application never uses it, it won't be a problem.
try:
    _pam_authenticate = libpam.pam_authenticate
    _pam_authenticate.restype = c_int
    _pam_authenticate.argtypes = (PamHandle, c_int)
except Exception as e:
    _pam_authenticate = _pam_fn_unavailable("pam_authenticate", e)

try:
    _pam_setcred = libpam.pam_setcred
    _pam_setcred.restype = c_int
    _pam_setcred.argtypes = (PamHandle, c_int)
except Exception as e:
    _pam_setcred = _pam_fn_unavailable("pam_setcred", e)

try:
    _pam_acct_mgmt = libpam.pam_acct_mgmt
    _pam_acct_mgmt.restype = c_int
    _pam_acct_mgmt.argtypes = (PamHandle, c_int)
except Exception as e:
    _pam_acct_mgmt = _pam_fn_unavailable("pam_acct_mgmt", e)

try:
    _pam_open_session = libpam.pam_open_session
    _pam_open_session.restype = c_int
    _pam_open_session.argtypes = (PamHandle, c_int)

    _pam_close_session = libpam.pam_close_session
    _pam_close_session.restype = c_int
    _pam_close_session.argtypes = (PamHandle, c_int)
except Exception as e:
    _pam_open_session = _pam_fn_unavailable("pam_open_session", e)
    _pam_close_session = _pam_fn_unavailable("pam_close_session", e)
