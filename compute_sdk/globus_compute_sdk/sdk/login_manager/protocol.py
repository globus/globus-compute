from __future__ import annotations

from typing import Protocol, runtime_checkable

import globus_sdk

from ..web_client import WebClient


@runtime_checkable
class LoginManagerProtocol(Protocol):
    def ensure_logged_in(self) -> None: ...

    def logout(self) -> bool: ...

    def get_auth_client(self) -> globus_sdk.AuthClient: ...

    def get_web_client(self, *, base_url: str | None = None) -> WebClient: ...
