from __future__ import annotations

import sys

import globus_sdk

from ..web_client import FuncxWebClient

# these were added to stdlib typing in 3.8, so the import must be conditional
# mypy and other tools expect and document a sys.version_info check
if sys.version_info >= (3, 8):
    from typing import Protocol, runtime_checkable
else:
    from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class LoginManagerProtocol(Protocol):
    def ensure_logged_in(self) -> None:
        ...

    def logout(self) -> bool:
        ...

    def get_auth_client(self) -> globus_sdk.AuthClient:
        ...

    def get_search_client(self) -> globus_sdk.SearchClient:
        ...

    def get_funcx_web_client(self, *, base_url: str | None = None) -> FuncxWebClient:
        ...
