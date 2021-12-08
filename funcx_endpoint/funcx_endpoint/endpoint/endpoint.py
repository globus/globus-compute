import glob
import logging
import os
import pathlib
from importlib.machinery import SourceFileLoader

import typer

from funcx_endpoint.endpoint.endpoint_manager import EndpointManager
from funcx_endpoint.logging_config import setup_logging

app = typer.Typer()
log = logging.getLogger(__name__)


def version_callback(value):
    if value:
        import funcx_endpoint

        typer.echo(f"FuncX endpoint version: {funcx_endpoint.__version__}")
        raise typer.Exit()


def complete_endpoint_name():
    # Manager context is not initialized at this point, so we assume the default
    # the funcx_dir path of ~/.funcx
    funcx_dir = os.path.join(pathlib.Path.home(), ".funcx")
    config_files = glob.glob(os.path.join(funcx_dir, "*", "config.py"))
    for config_file in config_files:
        yield os.path.basename(os.path.dirname(config_file))


@app.command(name="configure", help="Configure an endpoint")
def configure_endpoint(
    name: str = typer.Argument(
        "default", help="endpoint name", autocompletion=complete_endpoint_name
    ),
    endpoint_config: str = typer.Option(
        None, "--endpoint-config", help="endpoint config file"
    ),
):
    """Configure an endpoint

    Drops a config.py template into the funcx configs directory.
    The template usually goes to ~/.funcx/<ENDPOINT_NAME>/config.py
    """
    manager.configure_endpoint(name, endpoint_config)


@app.command(name="start", help="Start an endpoint by name")
def start_endpoint(
    name: str = typer.Argument("default", autocompletion=complete_endpoint_name),
    endpoint_uuid: str = typer.Option(
        None, help="The UUID for the endpoint to register with"
    ),
):
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

    Parameters
    ----------
    name : str
    endpoint_uuid : str
    """
    endpoint_dir = os.path.join(manager.funcx_dir, name)

    if not os.path.exists(endpoint_dir):
        msg = (
            f"\nEndpoint {name} is not configured!\n"
            "1. Please create a configuration template with:\n"
            f"\tfuncx-endpoint configure {name}\n"
            "2. Update the configuration\n"
            "3. Start the endpoint\n"
        )
        print(msg)
        return

    try:
        endpoint_config = SourceFileLoader(
            "config", os.path.join(endpoint_dir, manager.funcx_config_file_name)
        ).load_module()
    except Exception:
        log.exception(
            "funcX v0.2.0 made several non-backwards compatible changes to the config. "
            "Your config might be out of date. "
            "Refer to "
            "https://funcx.readthedocs.io/en/latest/endpoints.html#configuring-funcx"
        )
        raise

    manager.start_endpoint(name, endpoint_uuid, endpoint_config)


@app.command(name="stop")
def stop_endpoint(
    name: str = typer.Argument("default", autocompletion=complete_endpoint_name)
):
    """Stops an endpoint using the pidfile"""

    manager.stop_endpoint(name)


@app.command(name="restart")
def restart_endpoint(
    name: str = typer.Argument("default", autocompletion=complete_endpoint_name)
):
    """Restarts an endpoint"""
    stop_endpoint(name)
    start_endpoint(name)


@app.command(name="list")
def list_endpoints():
    """List all available endpoints"""
    manager.list_endpoints()


@app.command(name="delete")
def delete_endpoint(
    name: str = typer.Argument(..., autocompletion=complete_endpoint_name),
    autoconfirm: bool = typer.Option(
        False, "-y", help="Do not ask for confirmation to delete."
    ),
):
    """Deletes an endpoint and its config."""
    if not autoconfirm:
        typer.confirm(
            f"Are you sure you want to delete the endpoint <{name}>?", abort=True
        )

    manager.delete_endpoint(name)


@app.callback()
def main(
    ctx: typer.Context,
    _: bool = typer.Option(
        None, "--version", "-v", callback=version_callback, is_eager=True
    ),
    debug: bool = typer.Option(False, "--debug", "-d"),
    config_dir: str = typer.Option(
        os.path.join(pathlib.Path.home(), ".funcx"),
        "--config_dir",
        "-c",
        help="override default config dir",
    ),
):
    # Note: no docstring here; the docstring for @app.callback is used as a help
    # message for overall app.
    #
    # Sets up global variables in the State wrapper (debug flag, config dir, default
    # config file).
    #
    # For commands other than `init`, we ensure the existence of the config directory
    # and file.

    setup_logging(debug=debug)
    log.debug("Command: %s", ctx.invoked_subcommand)

    global manager
    manager = EndpointManager(funcx_dir=config_dir, debug=debug)

    # Otherwise, we ensure that configs exist
    if not os.path.exists(manager.funcx_config_file):
        log.info(
            "No existing configuration found at %s. Initializing...",
            manager.funcx_config_file,
        )
        manager.init_endpoint()

    log.debug(f"Loading config files from {manager.funcx_dir}")

    funcx_config = SourceFileLoader(
        "global_config", manager.funcx_config_file
    ).load_module()
    manager.funcx_config = funcx_config.global_options


def cli_run():
    """Entry point for setuptools to point to"""
    app()


if __name__ == "__main__":
    app()
