from __future__ import annotations

import difflib
import importlib.util
import json
import logging
import pathlib
import shutil
import sys
import uuid

import click
from click import ClickException
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_endpoint.endpoint.endpoint_manager import EndpointManager
from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.logging_config import setup_logging
from globus_compute_endpoint.version import DEPRECATION_FUNCX_ENDPOINT
from globus_compute_sdk.sdk.login_manager import LoginManager
from globus_compute_sdk.sdk.login_manager.tokenstore import ensure_compute_dir
from globus_compute_sdk.sdk.login_manager.whoami import print_whoami_info
from packaging.version import Version

log = logging.getLogger(__name__)


class CommandState:
    def __init__(self):
        self.endpoint_config_dir: pathlib.Path = init_config_dir()
        self.debug = False
        self.no_color = False
        self.log_to_console = False
        self.endpoint_uuid = None
        self.die_with_parent = False

    @classmethod
    def ensure(cls) -> CommandState:
        return click.get_current_context().ensure_object(CommandState)


def init_config_dir() -> pathlib.Path:
    try:
        return ensure_compute_dir()
    except (FileExistsError, PermissionError) as e:
        raise ClickException(str(e))


def get_config_dir() -> pathlib.Path:
    state = CommandState.ensure()
    return state.endpoint_config_dir


def get_cli_endpoint() -> Endpoint:
    # this getter creates an Endpoint object from the CommandState
    # it takes its various configurable values from the current CommandState
    # as a result, any number of CLI options may be used to tweak the CommandState
    # via callbacks, and the Endpoint will only be constructed within commands which
    # access the Endpoint via this getter
    state = CommandState.ensure()
    endpoint = Endpoint(debug=state.debug)

    return endpoint


def set_param_to_config(ctx, param, value):
    state = CommandState.ensure()
    setattr(state, param.name, value)
    return state


def log_flag_callback(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    state = set_param_to_config(ctx, param, value)
    setup_logging(debug=state.debug, no_color=state.no_color)


def common_options(f):
    f = click.option(
        "--debug",
        is_flag=True,
        expose_value=False,
        callback=log_flag_callback,
        is_eager=True,
        help="Emit extra information generally only helpful for developers",
    )(f)
    f = click.option(
        "--log-to-console",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        callback=log_flag_callback,
        help="Emit log lines to console as well as log",
    )(f)
    f = click.option(
        "--no-color",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        callback=log_flag_callback,
        help="Do not colorize the (console) log lines",
    )(f)

    f = click.help_option("-h", "--help")(f)
    return f


def start_options(f):
    f = click.option(
        "--endpoint-uuid",
        default=None,
        callback=set_param_to_config,
        help="The UUID to register with the Globus Compute services",
    )(f)
    f = click.option(
        "--die-with-parent",
        is_flag=True,
        hidden=True,
        callback=set_param_to_config,
        help="Shutdown if parent process goes away",
    )(f)
    return f


def name_arg(f):
    return click.argument("name", required=False, default="default")(f)


def config_dir_callback(ctx, param, value):
    if value is None or ctx.resilient_parsing:
        return
    state = CommandState.ensure()
    state.endpoint_config_dir = pathlib.Path(value)


@click.group("globus-compute-endpoint")
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
    setup_logging()  # Until we parse the CLI flags, just setup default logging


@app.command("version")
@common_options
def version_command():
    """Show the version of globus-compute-endpoint"""
    import globus_compute_endpoint as gce

    click.echo(f"Globus Compute endpoint version: {gce.__version__}")


@app.command(name="configure", help="Configure an endpoint")
@click.option("--endpoint-config", default=None, help="a template config to use")
@click.option(
    "--multi-tenant",
    is_flag=True,
    default=False,
    hidden=True,
    help="Configure endpoint as multi-tenant capable",
)
@click.option(
    "--display-name",
    help="A human readable display name for the endpoint, if desired",
)
@name_arg
@common_options
def configure_endpoint(
    *,
    name: str,
    endpoint_config: str | None,
    multi_tenant: bool,
    display_name: str | None,
):
    """Configure an endpoint

    Drops a config.py template into the Globus Compute configs directory.
    The template usually goes to ~/.globus_compute/<ENDPOINT_NAME>/config.py
    """
    try:
        Endpoint.validate_endpoint_name(name)
    except ValueError as e:
        raise ClickException(str(e))
    compute_dir = get_config_dir()
    ep_dir = compute_dir / name
    Endpoint.configure_endpoint(ep_dir, endpoint_config, multi_tenant, display_name)


@app.command(name="start", help="Start an endpoint by name")
@name_arg
@start_options
@common_options
def start_endpoint(*, name: str, **_kwargs):
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
    state = CommandState.ensure()
    _do_start_endpoint(
        name=name,
        endpoint_uuid=state.endpoint_uuid,
        die_with_parent=state.die_with_parent,
    )


@app.command(name="logout", help="Logout from all endpoints")
@click.option(
    "--force",
    is_flag=True,
    help="Revokes tokens even with currently running endpoints",
)
def logout_endpoints(force: bool):
    success, msg = _do_logout_endpoints(force=force)
    if not success:
        # Raising ClickException is apparently the way to do sys.exit(1)
        #     and return a non-zero value to the command line
        # See https://click.palletsprojects.com/en/8.1.x/exceptions/
        if not isinstance(msg, str) or msg is None:
            # Generic unsuccessful if no reason was given
            msg = "Logout unsuccessful"
        raise ClickException(msg)


@app.command("whoami", help="Show the currently logged-in identity")
@click.option(
    "--linked-identities",
    is_flag=True,
    default=False,
    help="Also show identities linked to the currently logged-in primary identity.",
)
def whoami(linked_identities: bool) -> None:
    try:
        print_whoami_info(linked_identities)
    except ValueError as ve:
        raise ClickException(str(ve))


def _do_logout_endpoints(
    force: bool, running_endpoints: dict | None = None
) -> tuple[bool, str | None]:
    """
    Logout from all endpoints and remove cached authentication credentials

    Returns True, None if logout was successful and tokens were found and revoked
    Returns False, error_msg if token revocation was not done
    """
    if running_endpoints is None:
        compute_dir = get_config_dir()
        running_endpoints = Endpoint.get_running_endpoints(compute_dir)
    tokens_revoked = False
    error_msg = None
    if running_endpoints and not force:
        running_list = ", ".join(running_endpoints.keys())
        log.info(
            "The following endpoints are currently running: "
            + running_list
            + "\nPlease use logout --force to proceed"
        )
        error_msg = "Not logging out with running endpoints without --force"
    else:
        tokens_revoked = LoginManager().logout()
        if tokens_revoked:
            log.info("Logout succeeded and all cached credentials were revoked")
        else:
            error_msg = "No cached tokens were found, already logged out?"
            log.info(error_msg)
    return tokens_revoked, error_msg


FUNCX_COMPUTE_IMPORT_UPDATES = {
    "from funcx_endpoint.endpoint.utils.config": "from globus_compute_endpoint.endpoint.utils.config",  # noqa E501
    "from funcx_endpoint.executors": "from globus_compute_endpoint.executors",  # noqa E501
}


def _upgrade_funcx_imports_in_config(name: str, force=False) -> str:
    """
    This only modifies unindented import lines, as are in the original
    config.py.  Indented matching lines are user created and would have to be
    updated manually by the user.

    The force flag will overwrite an existing (non-directory) config.py.bak

    This method uses a randomly generated intermediate output file in case
    there are any permission or unforeseen file system issues
    """
    ep_dir = get_config_dir() / name
    config_path = ep_dir / "config.py"
    config_backup = ep_dir / "config.py.bak"

    try:
        config_text = config_path.read_text()
        upd_config_text = config_text

        for original, repl in FUNCX_COMPUTE_IMPORT_UPDATES.items():
            upd_config_text = upd_config_text.replace(original, repl)

        if upd_config_text == config_text:
            return f"No funcX import statements found in config.py for {name}"

        change_diff = "".join(
            difflib.unified_diff(
                config_text.splitlines(keepends=True),
                upd_config_text.splitlines(keepends=True),
                n=3,  # Typical 3 lines of context
            )
        )

        if config_backup.exists() and not force:
            msg = (
                f"{config_backup} already exists.\n"
                "Rename it or use the --force flag to update config."
            )
            raise ClickException(msg)
        elif config_backup.is_dir():
            msg = (
                f"{config_backup} is a directory.\n"
                "Rename it before proceeding with config update."
            )
            raise ClickException(msg)

        # Write to temporary file in case of issues
        tmp_output_path = ep_dir / ("config.py." + uuid.uuid4().hex)
        tmp_output_path.write_text(upd_config_text)

        # Rename files last, as it's the least likely to err
        config_backup.unlink(missing_ok=True)
        shutil.move(config_path, config_backup)  # Preserve file timestamp
        shutil.move(tmp_output_path, config_path)

        return (
            f"Applied following diff for endpoint {name}:\n{change_diff}\n\n"
            f"  The previous config has been renamed to {config_backup}"
        )

    except FileNotFoundError as err:
        msg = f"No config.py was found for endpoint ({name}) in {ep_dir}"
        raise ClickException(msg) from err
    except ClickException:
        raise
    except Exception as err:
        msg = f"Unknown Exception {err} attempting to reformat config.py in {ep_dir}"
        raise ClickException(msg) from err


def read_config(endpoint_dir: pathlib.Path) -> Config:
    endpoint_name = endpoint_dir.name

    try:
        from funcx_endpoint.version import VERSION

        if Version(VERSION) < Version("2.0.0"):
            msg = (
                "To avoid compatibility issues with Globus Compute, please uninstall "
                "funcx-endpoint or upgrade funcx-endpoint to >=2.0.0. Note that the "
                "funcx-endpoint package is now deprecated."
            )
            raise ClickException(msg)
    except ModuleNotFoundError:
        pass

    try:
        conf_path = endpoint_dir / "config.py"
        spec = importlib.util.spec_from_file_location("config", conf_path)
        if not (spec and spec.loader):
            raise Exception(f"Unable to import configuration (no spec): {conf_path}")
        config = importlib.util.module_from_spec(spec)
        if not config:
            raise Exception(f"Unable to import configuration (no config): {conf_path}")
        spec.loader.exec_module(config)
        return config.config

    except FileNotFoundError as err:
        if not endpoint_dir.exists():
            configure_command = "globus-compute-endpoint configure"
            if endpoint_name != "default":
                configure_command += f" {endpoint_name}"
            msg = (
                f"{err}"
                f"\n\nEndpoint '{endpoint_name}' is not configured!"
                "\n1. Please create a configuration template with:"
                f"\n\t{configure_command}"
                "\n2. Update the configuration"
                "\n3. Start the endpoint\n"
            )
            raise ClickException(msg) from err
        msg = (
            f"{err}"
            "\n\nUnable to find required configuration file; has the configuration"
            "\ndirectory been corrupted?"
        )
        raise ClickException(msg) from err

    except AttributeError as err:
        msg = (
            f"{err}"
            "\n\nFailed to find expected data structure in configuration file."
            "\nHas the configuration file been corrupted or modified incorrectly?\n"
        )
        raise ClickException(msg) from err

    except ModuleNotFoundError as err:
        # Catch specific error when old config.py references funcx_endpoint
        if "No module named 'funcx_endpoint'" in err.msg:
            msg = (
                f"{conf_path} contains import statements from a previously "
                "configured endpoint that uses the (deprecated) "
                "funcx-endpoint library. Please update the imports to reference "
                "globus_compute_endpoint.\n\ni.e.\n"
                "    from funcx_endpoint.endpoint.utils.config -> "
                "from globus_compute_endpoint.endpoint.utils.config\n"
                "    from funcx_endpoint.executors -> "
                "from globus_compute_endpoint.executors\n"
                "\n"
                "You can also use the command "
                "`globus-compute-endpoint update_funcx_config [endpoint_name]` "
                "to update them\n"
            )
            raise ClickException(msg) from err
        else:
            log.exception(err.msg)
            raise

    except Exception:
        log.exception(
            "Globus Compute v2.0.0 made several non-backwards compatible changes to "
            "the config. Your config might be out of date. "
            "Refer to "
            "https://funcx.readthedocs.io/en/latest/endpoints.html#configuring-funcx"
        )
        raise


def _do_start_endpoint(
    *,
    name: str,
    endpoint_uuid: str | None,
    die_with_parent: bool = False,
):
    state = CommandState.ensure()
    ep_dir = get_config_dir() / name
    if ep_dir.is_dir():
        setup_logging(
            logfile=ep_dir / "endpoint.log",
            debug=state.debug,
            console_enabled=state.log_to_console,
            no_color=state.no_color,
        )

    reg_info = {}
    if sys.stdin and not (sys.stdin.closed or sys.stdin.isatty()):
        try:
            stdin_data = json.loads(sys.stdin.read())
            if not isinstance(stdin_data, dict):
                type_name = stdin_data.__class__.__name__
                raise ValueError(
                    "Expecting JSON dictionary with endpoint registration info; got"
                    f" {type_name} instead"
                )
            reg_info = stdin_data
        except Exception as e:
            exc_type = e.__class__.__name__
            log.debug("Invalid registration info on stdin -- (%s) %s", exc_type, e)

    ep_config = read_config(ep_dir)
    if ep_config.multi_tenant:
        epm = EndpointManager(ep_dir, endpoint_uuid, ep_config)
        epm.start()
    else:
        get_cli_endpoint().start_endpoint(
            ep_dir,
            endpoint_uuid,
            ep_config,
            state.log_to_console,
            state.no_color,
            reg_info,
            die_with_parent,
        )


@app.command("stop")
@name_arg
@click.option(
    "--remote",
    is_flag=True,
    help="send stop signal to all endpoints with this UUID, local or elsewhere",
)
@common_options
def stop_endpoint(*, name: str, remote: bool):
    """Stops an endpoint using the pidfile"""
    _do_stop_endpoint(name=name, remote=remote)


@app.command("update_funcx_config")
@name_arg
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="update config and backup to config.py.bak even if it already exists",
)
def update_funcx_endpoint_config(*, name: str, force: bool):
    """
    Update imports file from funcx_endpoint.* to globus_compute_endpoint.*

    Either should raise ClickException or returns modification result message
    """
    print(_upgrade_funcx_imports_in_config(name=name, force=force))


def _do_stop_endpoint(*, name: str, remote: bool = False) -> None:
    ep_dir = get_config_dir() / name
    Endpoint.stop_endpoint(ep_dir, read_config(ep_dir), remote=remote)


@app.command("restart")
@name_arg
@common_options
@start_options
def restart_endpoint(*, name: str, **_kwargs):
    """Restarts an endpoint"""
    state = CommandState.ensure()
    _do_stop_endpoint(name=name)
    _do_start_endpoint(
        name=name,
        endpoint_uuid=state.endpoint_uuid,
        die_with_parent=state.die_with_parent,
    )


@app.command("list")
@common_options
def list_endpoints():
    """List all available endpoints"""
    compute_dir = get_config_dir()
    Endpoint.print_endpoint_table(compute_dir)


@app.command("delete")
@name_arg
@click.option(
    "--force",
    default=False,
    is_flag=True,
    help="Deletes the local endpoint even if the web service returns an error.",
)
@click.option(
    "--yes", default=False, is_flag=True, help="Do not ask for confirmation to delete."
)
@common_options
def delete_endpoint(*, name: str, force: bool, yes: bool):
    """Deletes an endpoint and its config."""
    if not yes:
        click.confirm(
            f"Are you sure you want to delete the endpoint <{name}>?", abort=True
        )

    ep_dir = get_config_dir() / name
    Endpoint.delete_endpoint(ep_dir, read_config(ep_dir), force)


def cli_run():
    """Entry point for setuptools to point to"""
    app()


def cli_run_funcx():
    """Entry point that prints a custom message. i.e. deprecation warnings"""
    fmt = DEPRECATION_FUNCX_ENDPOINT

    # Colorized notice to be a bit more visible
    fmt = "{title}DEPRECATION NOTICE{rs}\n{body}" + fmt + "{rs}"
    title = frs = body = rs = ""
    if sys.stderr.isatty():
        title = "\033[37;41m"  # White FG, Red BG
        body = "\033[33m"  # Yellow FG
        rs = "\033[0m"  # Reset colors

    print(fmt.format(title=title, body=body, frs=frs, rs=rs), file=sys.stderr)
    app()


if __name__ == "__main__":
    app()
