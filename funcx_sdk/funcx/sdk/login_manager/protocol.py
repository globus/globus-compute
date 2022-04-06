from __future__ import annotations

import typing as t

import globus_sdk

from ..web_client import FuncxWebClient


class LoginManagerProtocol(t.Protocol):
    def ensure_logged_in(self) -> None:
        ...

    def logout(self) -> None:
        ...

    def get_auth_client(self) -> globus_sdk.AuthClient:
        ...

    def get_search_client(self) -> globus_sdk.SearchClient:
        ...

    def get_funcx_web_client(self, *, base_url: str | None = None) -> FuncxWebClient:
        ...
