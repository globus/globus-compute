import os
import typing as t


def _get_envname():
    return os.getenv("FUNCX_SDK_ENVIRONMENT", "production")


def get_web_service_url(envname: t.Optional[str]) -> str:
    env = envname or _get_envname()
    prod = "https://api2.funcx.org/v2"
    return {
        "production": prod,
        "dev": "https://api.dev.funcx.org/v2",
    }.get(env, prod)


def get_web_socket_url(envname: t.Optional[str]) -> str:
    env = envname or _get_envname()
    prod = "wss://api2.funcx.org/ws/v2/"
    return {
        "production": prod,
        "dev": "wss://api.dev.funcx.org/ws/v2/",
    }.get(env, prod)
