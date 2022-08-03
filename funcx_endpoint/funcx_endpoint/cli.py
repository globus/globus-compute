from __future__ import annotations

import logging
import os
import pathlib
from importlib.machinery import SourceFileLoader

import click

from .endpoint.endpoint import Endpoint
from .logging_config import setup_logging

log = logging.getLogger(__name__)


class CommandState:
    def __init__(self):
        self.endpoint_config_dir: str = str(pathlib.Path.home() / ".funcx")
        self.debug = False

    @classmethod
    def ensure(cls) -> CommandState:
        return click.get_current_context().ensure_object(CommandState)


def get_cli_endpoint() -> Endpoint:
    # this getter creates an Endpoint object from the CommandState
    # it takes its various configurable values from the current CommandState
    # as a result, any number of CLI options may be used to tweak the CommandState
    # via callbacks, and the Endpoint will only be constructed within commands which
    # access the Endpoint via this getter
    state = CommandState.ensure()

    endpoint = Endpoint(funcx_dir=state.endpoint_config_dir, debug=state.debug)

    # ensure that configs exist
    if not os.path.exists(endpoint.funcx_dir):
        log.info(
            "No existing configuration found at %s. Initializing...", endpoint.funcx_dir
        )
        endpoint.init_endpoint()

    return endpoint


def common_options(f):
    def debug_callback(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return

        state = CommandState.ensure()
        state.debug = True

    f = click.option(
        "--debug",
        is_flag=True,
        hidden=True,
        expose_value=False,
        callback=debug_callback,
        is_eager=True,
    )(f)
    f = click.help_option("-h", "--help")(f)
    return f


def name_arg(f):
    return click.argument("name", required=False, default="default")(f)


def config_dir_callback(ctx, param, value):
    if value is None or ctx.resilient_parsing:
        return
    state = CommandState.ensure()
    state.endpoint_config_dir = value


@click.group("funcx-endpoint")
@common_options
@click.option(
    "-c",
    "--config-dir",
    default=None,
    help="override default config dir",
    callback=config_dir_callback,
    expose_value=False,
)
def app():
    # the main command group body runs on every command, so the block below will always
    # execute
    state = CommandState.ensure()
    setup_logging(debug=state.debug)


@app.command("version")
@common_options
def version_command():
    """Show the version of funcx-endpoint"""
    import funcx_endpoint

    click.echo(f"FuncX endpoint version: {funcx_endpoint.__version__}")


@app.command(name="configure", help="Configure an endpoint")
@click.option("--endpoint-config", default=None, help="a template config to use")
@name_arg
@common_options
def configure_endpoint(*, name: str, endpoint_config: str | None):
    """Configure an endpoint

    Drops a config.py template into the funcx configs directory.
    The template usually goes to ~/.funcx/<ENDPOINT_NAME>/config.py
    """
    endpoint = get_cli_endpoint()
    endpoint.configure_endpoint(name, endpoint_config)


@app.command(name="start", help="Start an endpoint by name")
@name_arg
@click.option(
    "--endpoint-uuid", default=None, help="The UUID for the endpoint to register with"
)
@common_options
def start_endpoint(*, name: str, endpoint_uuid: str | None):
    """Start an endpoint

    This function will do:
    1. Connect to the broker service, and register itself
    2. Get connection info from broker service
    3. Start the interchange as a daemon


    |                      Broker service       |
    |               -----2----> Forwarder       |
    |    /register <-----3----+   ^             |
    +-----^-----------------------+-------------+
          |     |                 |
          1     4                 6
          |     v                 |
    +-----+-----+-----+           v
    |      Start      |---5---> EndpointInterchange
    |     Endpoint    |         daemon
    +-----------------+
    """
    _do_start_endpoint(name=name, endpoint_uuid=endpoint_uuid)


def _do_start_endpoint(*, name: str, endpoint_uuid: str | None):
    endpoint = get_cli_endpoint()
    endpoint_dir = os.path.join(endpoint.funcx_dir, name)

    if not os.path.exists(endpoint_dir):
        configure_command = "funcx-endpoint configure"
        if name != "default":
            configure_command += f" -n {name}"
        msg = (
            f"\nEndpoint {name} is not configured!\n"
            "1. Please create a configuration template with:\n"
            f"\t{configure_command}\n"
            "2. Update the configuration\n"
            "3. Start the endpoint\n"
        )
        click.echo(msg)
        return

    try:
        endpoint_config = SourceFileLoader(
            "config", os.path.join(endpoint_dir, "config.py")
        ).load_module()
    except Exception:
        log.exception(
            "funcX v0.2.0 made several non-backwards compatible changes to the config. "
            "Your config might be out of date. "
            "Refer to "
            "https://funcx.readthedocs.io/en/latest/endpoints.html#configuring-funcx"
        )
        raise
    endpoint.start_endpoint(name, endpoint_uuid, endpoint_config)


@app.command("stop")
@name_arg
@common_options
def stop_endpoint(*, name: str):
    """Stops an endpoint using the pidfile"""
    _do_stop_endpoint(name=name)


def _do_stop_endpoint(*, name: str) -> None:
    endpoint = get_cli_endpoint()
    endpoint.stop_endpoint(name)


@app.command("restart")
@name_arg
@click.option("--endpoint-uuid", default=None, hidden=True)
@common_options
def restart_endpoint(*, name: str, endpoint_uuid: str | None):
    """Restarts an endpoint"""
    _do_stop_endpoint(name=name)
    _do_start_endpoint(name=name, endpoint_uuid=endpoint_uuid)


@app.command("list")
@common_options
def list_endpoints():
    """List all available endpoints"""
    endpoint = get_cli_endpoint()
    endpoint.list_endpoints()


@app.command("delete")
@name_arg
@click.option("--yes", is_flag=True, help="Do not ask for confirmation to delete.")
@common_options
def delete_endpoint(*, name: str, yes: bool):
    """Deletes an endpoint and its config."""
    if not yes:
        click.confirm(
            f"Are you sure you want to delete the endpoint <{name}>?", abort=True
        )

    endpoint = get_cli_endpoint()
    endpoint.delete_endpoint(name)


def cli_run():
    """Entry point for setuptools to point to"""
    app()


if __name__ == "__main__":
    app()
