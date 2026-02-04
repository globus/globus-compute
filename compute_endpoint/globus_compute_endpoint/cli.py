from __future__ import annotations

import contextlib
import json
import logging
import os
import pathlib
import pwd
import shutil
import signal
import sys
import textwrap
import threading
import time
import typing as t
import uuid
import warnings
from dataclasses import asdict

import click
import lockfile
from click import ClickException
from click_option_group import optgroup
from daemon.pidfile import PIDLockFile
from globus_compute_endpoint.auth import get_globus_app_with_scopes
from globus_compute_endpoint.boot_persistence import disable_on_boot, enable_on_boot
from globus_compute_endpoint.endpoint.config import (
    ManagerEndpointConfig,
    UserEndpointConfig,
)
from globus_compute_endpoint.endpoint.config.utils import (
    get_config,
    load_config_yaml,
    load_user_config_schema,
    load_user_config_template,
    render_config_user_template,
)
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_endpoint.endpoint.endpoint_manager import EndpointManager
from globus_compute_endpoint.endpoint.identity_mapper import MappedPosixIdentity
from globus_compute_endpoint.endpoint.utils import has_pyprctl as _has_multi_user
from globus_compute_endpoint.endpoint.utils import (
    is_privileged,
    pyprctl_import_error,
    send_endpoint_startup_failure_to_amqp,
    user_input_select,
)
from globus_compute_endpoint.exception_handling import handle_auth_errors
from globus_compute_endpoint.logging_config import setup_logging
from globus_compute_sdk.sdk.auth.auth_client import ComputeAuthClient
from globus_compute_sdk.sdk.auth.whoami import print_whoami_info
from globus_compute_sdk.sdk.batch import create_user_runtime
from globus_compute_sdk.sdk.compute_dir import ensure_compute_dir
from globus_compute_sdk.sdk.utils.gare import gare_handler
from globus_sdk import MISSING, AuthClient, GlobusAPIError, MissingType

if not _has_multi_user and "--debug" in sys.argv and sys.stderr.isatty():
    # We haven't set up logging yet so manually print for now
    _e = pyprctl_import_error
    print(
        f"(DEBUG) {__file__}: pyprctl not available; MEPs will not work"
        "\n(DEBUG) (Hints: Is pyprctl installed? Is this a Linux system?)"
        f"\n(DEBUG) [{type(_e).__name__}] {_e}",
        file=sys.stderr,
    )

log = logging.getLogger(__name__)


class ClickExceptionWithContext(ClickException):
    def __init__(self, message: str) -> None:
        super().__init__(message)

    def format_message(self) -> str:
        msg = super().format_message()
        if (e := self.__cause__) is not None:
            msg += f"\n\t({e.__class__.__module__}.{e.__class__.__name__}) {e}"
        return msg

    def __str__(self) -> str:
        return self.format_message()


_AUTH_POLICY_DEFAULT_NAME = "Globus Compute Authentication Policy"
_AUTH_POLICY_DEFAULT_DESC = "This policy was created automatically by Globus Compute."


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


def get_cli_endpoint(conf: UserEndpointConfig) -> Endpoint:
    # this getter creates an Endpoint object from the CommandState
    # it takes its various configurable values from the current CommandState
    # as a result, any number of CLI options may be used to tweak the CommandState
    # via callbacks, and the Endpoint will only be constructed within commands which
    # access the Endpoint via this getter
    state = CommandState.ensure()
    endpoint = Endpoint(debug=state.debug or conf.debug)

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


def get_ep_dir_by_name_or_uuid(ctx, param, value, require_local: bool = True):
    """
    :param require_local:  Whether a local directory needs to be present,
                             specified by name or found via provided UUID.
                             If this is False, an unrecognized UUID is ok,
                             presumably used to interact with the web-service.
    """
    if not value:
        ctx.params["ep_dir"] = None
        return

    conf_dir = get_config_dir()
    try:
        uuid.UUID(value)
    except ValueError:
        # value is a name
        path = conf_dir / value
        uuid_provided = False
    else:
        uuid_provided = True
        path = Endpoint.get_endpoint_dir_by_uuid(conf_dir, value)

        # pass the UUID value as a param to be used later, but only if
        # the endpoint isn't necessarily local.  This require_local check
        # also avoids adding the `ep_uuid` as a param to some endpoint commands
        if not require_local:
            ctx.params["ep_uuid"] = value

    if (path and not path.exists()) or (path is None and require_local):
        # If a EP name is given but dir doesn't exist, or if a UUID
        # is given but a local dir is required but missing, error out.
        # (Otherwise, proceed with at least one of ep_dir/ep_uuid set)
        if uuid_provided:
            ep_info = f"with ID {value}"
            ep_name = "<endpoint_name>"
        else:
            ep_info = f"at {path}"
            ep_name = value

        msg = textwrap.dedent(f"""
            There is no endpoint configuration on this machine {ep_info}

            1. Please create a configuration template with:
                globus-compute-endpoint configure {ep_name}
            2. Update the configuration
            3. Start the endpoint
            """)
        raise ClickException(msg)

    ctx.params["ep_dir"] = path


def get_ep_dir_uuid_only_ok(ctx, param, value):
    # A version of get_ep_dir that allows a UUID without a local dir present
    get_ep_dir_by_name_or_uuid(ctx, param, value, require_local=False)


def verify_not_uuid(ctx, param, value):
    try:
        uuid.UUID(value)
        raise click.BadParameter(
            f"'{ctx.command_path}' requires an endpoint name that"
            f" is not a UUID (got: '{value}')"
        )
    except ValueError:
        return value


def name_or_uuid_arg(f):
    return click.argument(
        "name_or_uuid",
        required=False,
        callback=get_ep_dir_by_name_or_uuid,
        default="default",
        expose_value=False,  # callback supplies ep_dir to command functions
    )(f)


def uuid_arg_or_local_dir(f):
    """
    A version of the decorator that allows UUIDs that do not have a local presence
    """
    return click.argument(
        "name_or_uuid",
        required=False,
        callback=get_ep_dir_uuid_only_ok,
        default="default",
        expose_value=False,  # callback supplies ep_dir/ep_uuid to command functions
    )(f)


def name_arg(f):
    return click.argument(
        "name", required=False, callback=verify_not_uuid, default="default"
    )(f)


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
@click.option(
    "--endpoint-config",
    default=None,
    help="DEPRECATED: use --manager-config or --template-config",
)
@click.option(
    "--manager-config",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
    help=(
        "An override to the default config.yaml,"
        " which is copied into the new endpoint directory"
    ),
)
@click.option(
    "--template-config",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
    help=(
        "An override to the default user_config_template.yaml.j2,"
        " which is copied into the new endpoint directory"
    ),
)
@click.option(
    "--schema-config",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
    help=(
        "An override to the default user_config_schema.json,"
        " which is copied into the new endpoint directory"
    ),
)
@click.option(
    "--multi-user",
    type=click.BOOL,
    is_flag=False,
    default=None,
    help=(
        "If true, configure endpoint with multi-user support; if false, configure"
        " without multi-user support.  If not set (the default), choose multi-user"
        " support based on configuring user's POSIX capability set."
    ),
)
@click.option(
    "--high-assurance",
    is_flag=True,
    default=False,
    help="Configure endpoint as High Assurance(HA) capable",
)
@click.option(
    "--display-name",
    help="A human readable display name for the endpoint, if desired",
)
@click.option(
    "--auth-policy",
    help=(
        "Endpoint users are evaluated against this Globus authentication policy. "
        "For more information on Globus authentication policies, visit "
        "https://docs.globus.org/api/auth/developer-guide/#authentication-policies."
    ),
)
@optgroup.group(
    "Authentication Policy Creation",
    help=(
        "Options for creating an auth policy. If any of these options are specified, "
        "the endpoint will be associated with an auth policy configured per those "
        "options and users will be evaluated against that policy."
    ),
)
@optgroup.option(
    "--auth-policy-project-id",
    help=(
        "The Globus Auth Project this policy should belong to "
        "(will prompt to create one if not given)"
    ),
    default=None,
)
@optgroup.option(
    "--auth-policy-display-name",
    default=_AUTH_POLICY_DEFAULT_NAME,
    show_default=True,
)
@optgroup.option(
    "--auth-policy-mfa-required",
    is_flag=True,
    default=False,
    help="Whether to require Multi-Factor Authentication on the policy",
)
@optgroup.option(
    "--auth-policy-description",
    default=_AUTH_POLICY_DEFAULT_DESC,
    show_default=True,
)
@optgroup.option(
    "--allowed-domains",
    help="A comma separated list of domains to include (ex: globus.org,*.edu)",
    default=None,
)
@optgroup.option(
    "--excluded-domains",
    help="A comma separated list of domains to exclude (ex: globus.org,*.edu)",
    default=None,
)
@optgroup.option(
    "--auth-timeout",
    help="How old (in seconds) a login session can be and still be compliant",
    type=click.IntRange(min=0),
    default=None,
)
@click.option("--subscription-id", help="Associate endpoint with a subscription")
@name_arg
@common_options
@handle_auth_errors
def configure_endpoint(
    *,
    name: str,
    endpoint_config: str | None,
    manager_config: pathlib.Path | None,
    template_config: pathlib.Path | None,
    schema_config: pathlib.Path | None,
    multi_user: bool | None,
    high_assurance: bool,
    display_name: str | None,
    auth_policy: str | None,
    auth_policy_mfa_required: bool,
    auth_policy_project_id: str | None,
    auth_policy_display_name: str,
    auth_policy_description: str,
    allowed_domains: str | None,
    excluded_domains: str | None,
    auth_timeout: int | None,
    subscription_id: str | None,
):
    """Configure an endpoint

    Drops a config.yaml template into the Globus Compute configs directory.
    The template usually goes to ~/.globus_compute/<ENDPOINT_NAME>/config.yaml
    """
    if not _has_multi_user:
        raise ClickException(
            "Unable to configure new endpoints; Manager Endpoint Processes are not"
            " supported on this system"
        )

    if endpoint_config is not None:
        warnings.warn(
            "--endpoint-config is deprecated; use --manager-config instead."
            " If you want to configure user endpoint processes, use --template-config.",
            DeprecationWarning,
        )

        if manager_config is None:
            manager_config = pathlib.Path(endpoint_config)
        else:
            warnings.warn(
                "Both --endpoint-config and --manager-config were provided;"
                " --endpoint-config will be ignored.",
                UserWarning,
            )

    try:
        Endpoint.validate_endpoint_name(name)
    except ValueError as e:
        raise ClickException(str(e))

    id_mapping = is_privileged() if multi_user is None else multi_user

    create_policy = (
        auth_policy_project_id is not None
        or auth_policy_display_name != _AUTH_POLICY_DEFAULT_NAME
        or auth_policy_description != _AUTH_POLICY_DEFAULT_DESC
        or allowed_domains is not None
        or excluded_domains is not None
        or auth_timeout is not None
    )

    if create_policy and auth_policy:
        raise ClickException(
            "Cannot specify an existing auth policy and "
            "create a new one at the same time"
        )
    elif high_assurance and not (subscription_id and (create_policy or auth_policy)):
        raise ClickException(
            "High Assurance(HA) endpoints require both a HA policy and "
            "a HA subscription id"
        )
    elif auth_policy_mfa_required and not create_policy:
        raise ClickException("MFA may only be specified when creating a policy")
    elif create_policy:
        if auth_policy_mfa_required and not high_assurance:
            raise ClickException(
                "MFA may only be enabled for High Assurance(HA) policies"
            )
        app = get_globus_app_with_scopes()
        ac = ComputeAuthClient(app=app)

        if not auth_policy_project_id:
            auth_policy_project_id = create_or_choose_auth_project(ac)

        # mypy is confused
        include_domain_list: list[str] | MissingType = (
            allowed_domains.split(",") if allowed_domains else MISSING
        )
        exclude_domain_list: list[str] | MissingType = (
            excluded_domains.split(",") if excluded_domains else MISSING
        )

        auth_policy = create_auth_policy(
            ac=ac,
            project_id=auth_policy_project_id,
            display_name=auth_policy_display_name,
            description=auth_policy_description,
            include_domains=include_domain_list,
            exclude_domains=exclude_domain_list,
            high_assurance=high_assurance or MISSING,
            timeout=auth_timeout or MISSING,
            require_mfa=auth_policy_mfa_required or MISSING,
        )

    compute_dir = get_config_dir()
    ep_dir = compute_dir / name
    Endpoint.configure_endpoint(
        conf_dir=ep_dir,
        endpoint_config=manager_config,
        user_config_template=template_config,
        user_config_schema=schema_config,
        id_mapping=id_mapping,
        high_assurance=high_assurance,
        display_name=display_name,
        auth_policy=auth_policy,
        subscription_id=subscription_id,
    )


@app.command(name="start", help="Start an endpoint")
@name_or_uuid_arg
@start_options
@common_options
@handle_auth_errors
def start_endpoint(*, ep_dir: pathlib.Path, **_kwargs):
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
        ep_dir=ep_dir,
        endpoint_uuid=state.endpoint_uuid,
        die_with_parent=state.die_with_parent,
    )


@app.command(name="login", help="Manually log in with Globus")
@click.option(
    "--force",
    is_flag=True,
    help="Log in even if already logged in",
)
def login(force: bool):
    _do_login(force)


@app.command(name="logout", help="Logout from all endpoints")
@click.option(
    "--force",
    is_flag=True,
    help="Revokes tokens even with currently running endpoints",
)
def logout_endpoints(force: bool):
    _do_logout_endpoints(force=force)


@app.command("whoami", help="Show the currently logged-in identity")
@click.option(
    "--linked-identities",
    is_flag=True,
    default=False,
    help="Also show identities linked to the currently logged-in primary identity.",
)
def whoami(linked_identities: bool) -> None:
    app = get_globus_app_with_scopes()
    try:
        print_whoami_info(app, linked_identities)
    except ValueError as ve:
        raise ClickException(str(ve))


def _do_login(force: bool) -> None:
    app = get_globus_app_with_scopes()
    if force or app.login_required():
        app.login(force=force)
    else:
        log.info("Already logged in. Use --force to run login flow anyway")


def _do_logout_endpoints(force: bool) -> None:
    """Logout from all endpoints and remove cached authentication credentials"""
    compute_dir = get_config_dir()
    running_endpoints = Endpoint.get_running_endpoints(compute_dir)

    if running_endpoints and not force:
        running_list = ", ".join(running_endpoints.keys())
        log.info(
            "The following endpoints are currently running: "
            + running_list
            + "\nPlease use logout --force to proceed"
        )
    else:
        app = get_globus_app_with_scopes()
        app.logout()
        log.info("Logout succeeded and all cached credentials were revoked")


@contextlib.contextmanager
def _pidfile(pid_path: pathlib.Path, stale_after_s: int | float):
    if pid_path.exists():
        pid_mtime = pid_path.stat().st_mtime
        _now = time.time()

        now_human = time.ctime(_now)
        mtime_human = time.ctime(pid_mtime)
        log.info(
            f"\n       PID path: {pid_path} (PID: {pid_path.read_text().strip()})"
            f"\n  Last modified: {pid_mtime:.3f} ({mtime_human})"
            f"\n   Current time: {_now:.3f} ({now_human})"
        )
        if _now - pid_mtime <= stale_after_s:
            msg = (
                "Another instance of this endpoint is running.  (Perhaps on"
                " another login node?)  Refusing to start.  If this is not"
                " correct, then remove the PID file before starting again."
            )
            log.error(msg)
            sys.exit(os.EX_CANTCREAT)

        else:
            log.warning(
                "Previous endpoint instance failed to shutdown cleanly."
                "  Removing PID file."
            )
            pid_path.unlink(missing_ok=True)

        del _now, stale_after_s, pid_mtime, now_human, mtime_human

    shutting_down = threading.Event()

    def _touch_pid():
        while not shutting_down.wait(15) and pid_path.exists():
            pid_path.touch(mode=0o644)
        pid_path.unlink(missing_ok=True)  # in case race condition

        if not shutting_down.is_set():
            # An external event has removed the PID file: we need to shut
            # down.
            mthread = threading.main_thread()
            log.warning("PID file removed by another process; initiating shutdown")
            os.kill(os.getpid(), signal.SIGTERM)
            mthread.join(timeout=15)
            if not mthread.is_alive():
                return

            # We're a daemon thread; if we're still here, then the SIGTERM
            # was not enough
            os.killpg(os.getpgrp(), signal.SIGTERM)
            threading.main_thread().join(timeout=5)

            if not mthread.is_alive():
                return

            # Damn.  Nuke it from orbit.
            os.killpg(os.getpgrp(), signal.SIGKILL)

    pidfile = PIDLockFile(pid_path)
    pidfile.acquire()
    threading.Thread(target=_touch_pid, daemon=True).start()
    try:
        yield
    finally:
        shutting_down.set()
        try:
            pidfile.release()
        except (lockfile.NotLocked, lockfile.NotMyLock):
            # Likely from a self-daemonization; regardless, we're shutting down
            # so no longer ours to worry about
            pass


def _do_start_endpoint(
    *,
    ep_dir: pathlib.Path,
    endpoint_uuid: str | None,
    die_with_parent: bool = False,
):
    os.umask(0o077)
    state = CommandState.ensure()
    if ep_dir.is_dir():
        setup_logging(
            logfile=ep_dir / "endpoint.log",
            debug=state.debug,
            console_enabled=state.log_to_console,
            no_color=state.no_color,
        )

    _no_fn_list_canary = -15  # an arbitrary random integer; invalid as an allow_list
    ep_info = {}
    reg_info = {}
    config_str: str | None = None
    audit_fd: int | None = None
    fn_allow_list: list[str] | None | int = _no_fn_list_canary
    if sys.stdin and not (sys.stdin.closed or sys.stdin.isatty()):
        try:
            stdin_data = json.loads(sys.stdin.read())

            if not isinstance(stdin_data, dict):
                type_name = stdin_data.__class__.__name__
                raise ValueError(
                    "Expecting JSON dictionary with endpoint info; got"
                    f" {type_name} instead"
                )

            ep_info = stdin_data.get("ep_info", {})
            reg_info = stdin_data.get("amqp_creds", {})
            config_str = stdin_data.get("config")
            audit_fd = stdin_data.get("audit_fd")
            fn_allow_list = stdin_data.get("allowed_functions", _no_fn_list_canary)

            del stdin_data  # clarity for intended scope

        except Exception as e:
            exc_type = e.__class__.__name__
            log.debug("Invalid info on stdin -- (%s) %s", exc_type, e)

    @contextlib.contextmanager
    def _send_message_on_error_exit():
        try:
            yield
        except (SystemExit, Exception) as e:
            if isinstance(e, SystemExit):
                if e.code in (0, None):
                    # normal, system exit
                    raise

            if reg_info:
                # We're quitting anyway, so just let any exceptions bubble
                msg = (
                    f"Failed to start or unexpected error:\n  ({type(e).__name__}) {e}"
                )
                send_endpoint_startup_failure_to_amqp(reg_info, msg=msg)

            raise

    with contextlib.ExitStack() as stk:
        stk.enter_context(_send_message_on_error_exit())
        try:
            if config_str is not None:
                ep_config = load_config_yaml(config_str)
            else:
                ep_config = get_config(ep_dir)
            del config_str

            if fn_allow_list != _no_fn_list_canary:
                ep_config.allowed_functions = fn_allow_list

            if not state.debug and ep_config.debug:
                setup_logging(
                    logfile=ep_dir / "endpoint.log",
                    debug=ep_config.debug,
                    console_enabled=state.log_to_console,
                    no_color=state.no_color,
                )

        except Exception as e:
            if isinstance(e, ClickException):
                raise

            # We've likely not exported to the log, so at least put _something_ in the
            # logs for the human to debug; motivated by SC-28607
            exc_type = type(e).__name__
            msg = (
                "Failed to find or parse endpoint configuration.  Endpoint will not"
                f" start. ({exc_type}) {e}"
            )
            log.critical(msg)
            raise

        pid_path = Endpoint.pid_path(ep_dir)
        stk.enter_context(_pidfile(pid_path, ep_config.heartbeat_period * 3))

        if isinstance(ep_config, ManagerEndpointConfig):
            if not _has_multi_user:
                raise ClickException(
                    "multi-user endpoints are not supported on this system"
                )
            epm = EndpointManager(ep_dir, endpoint_uuid, ep_config, reg_info)
            epm.start()
        else:
            assert isinstance(ep_config, UserEndpointConfig)

            if die_with_parent:
                # The endpoint cannot die with its parent if it doesn't have one :)
                ep_config.detach_endpoint = False
                log.debug("The --die-with-parent flag has set detach_endpoint to False")
            else:
                bname = os.path.basename(sys.argv[0])
                print(
                    "\nThis endpoint is not template capable.  To add that capability,"
                    " run:\n\n"
                    f"    $ {bname} migrate-to-template-capable {ep_dir.name}\n",
                    flush=True,
                )

            get_cli_endpoint(ep_config).start_endpoint(
                ep_dir,
                endpoint_uuid,
                ep_config,
                state.log_to_console,
                state.no_color,
                reg_info,
                ep_info,
                die_with_parent,
                audit_fd,
            )


@app.command("stop")
@name_or_uuid_arg
@click.option(
    "--remote",
    is_flag=True,
    help="send stop signal to all endpoints with this UUID, local or elsewhere",
)
@common_options
@handle_auth_errors
def stop_endpoint(*, ep_dir: pathlib.Path, remote: bool):
    """Stops an endpoint using the pidfile"""
    _do_stop_endpoint(ep_dir=ep_dir, remote=remote)


def _do_stop_endpoint(*, ep_dir: pathlib.Path, remote: bool = False) -> None:
    Endpoint.stop_endpoint(ep_dir, get_config(ep_dir), remote=remote)


@app.command("restart")
@name_or_uuid_arg
@common_options
@start_options
def restart_endpoint(*, ep_dir: pathlib.Path, **_kwargs):
    """Restarts an endpoint"""
    state = CommandState.ensure()
    _do_stop_endpoint(ep_dir=ep_dir)
    _do_start_endpoint(
        ep_dir=ep_dir,
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
@uuid_arg_or_local_dir
@click.option(
    "--force",
    default=False,
    is_flag=True,
    help=(
        "Ignores any errors encountered while attempting to delete an "
        "endpoint locally and remotely."
    ),
)
@click.option(
    "--yes",
    default=False,
    is_flag=True,
    help="Do not ask for confirmation to delete endpoints.",
)
@common_options
@handle_auth_errors
def delete_endpoint(
    *,
    ep_dir: pathlib.Path | None,
    ep_uuid: str | None = None,
    force: bool,
    yes: bool,
):
    """Deletes an endpoint and its config."""

    ep_info = f"< {ep_dir.name} >" if ep_dir else ""
    if ep_uuid:
        ep_info += f" ( {ep_uuid} )"

    if not yes:
        click.confirm(
            f"Are you sure you want to delete endpoint {ep_info}?",
            abort=True,
        )

    ep_conf = None
    error_msg = None

    if ep_dir:
        try:
            ep_conf = get_config(ep_dir)
            if ep_uuid is None:
                ep_uuid = Endpoint.get_endpoint_id(ep_dir)
        except Exception:
            # Endpoint directory exists locally but error loading the config
            error_msg = f"Failed to read configuration from {ep_dir}/ "
    else:
        error_msg = f"No local endpoint configuration available for {ep_uuid}"

    if error_msg:
        if force:
            log.info(f"{error_msg}, proceeding to remote deletion by --force ...")
        else:
            raise ClickException(
                f"{error_msg}\n\n"
                "Please add the --force flag if you wish to proceed with "
                "attempting to delete the endpoint both locally and from the "
                "Globus Compute web services."
            )

    Endpoint.delete_endpoint(ep_dir, ep_conf, force=force, ep_uuid=ep_uuid)


@app.command(
    "python-exec",
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
)
@click.argument("module")
@click.help_option("-h", "--help")
@click.pass_context
def run_python_executable(ctx: click.Context, module: str):
    """Run a Python module as a script via the Globus Compute endpoint CLI.
    This helps to minimize PATH issues when launching workers, etc.

    E.g., globus-compute-endpoint python-exec path.to.module --ahoy matey
    """
    os.execvpe(sys.executable, [sys.executable, "-m", module] + ctx.args, os.environ)


def create_or_choose_auth_project(ac: AuthClient) -> str:
    """
    Lets the user choose from one of their existing projects, or
    prompt for a description and creates one for them
    """
    assert ac._app is not None  # mypy
    projects = gare_handler(ac._app.login, ac.get_projects)
    if any(projects):
        proj_selected = user_input_select(
            (
                "Please select a project to contain new auth policy "
                "(leave blank to create a new one):"
            ),
            [f'id: {proj["id"]}, name: {proj["display_name"]}' for proj in projects],
        )
        if proj_selected:
            return proj_selected.split()[1][:-1]

    print("Creating a new auth project.")

    try:
        while True:
            display_name = input("Enter a name for this project: ")
            contact_email = input("Enter the contact email for this project: ")

            admin_ids_input = input(
                "Enter a comma separated list of admin IDs for this project: "
            )
            admin_ids = admin_ids_input.split(",") if admin_ids_input else []

            admin_gps_input = input(
                "Enter a comma separated list of group IDs as project admins: "
            )
            admin_group_ids = admin_gps_input.split(",") if admin_gps_input else []

            cont = input(
                "Create this auth project? "
                "(Y/y to confirm, Q/q to quit, otherwise start over): "
            ).lower()
            if cont == "y":
                break
            elif cont == "q":
                raise KeyboardInterrupt
            else:
                print()
                continue
    except KeyboardInterrupt:
        print("\nCanceled.")
        sys.exit(0)

    try:
        print("Creating project...")
        assert ac._app is not None  # mypy
        resp = gare_handler(
            ac._app.login,
            ac.create_project,
            display_name=display_name,
            contact_email=contact_email,
            admin_ids=admin_ids,
            admin_group_ids=admin_group_ids,
        )
        print("Project created.")
        return resp["project"]["id"]
    except GlobusAPIError as e:
        raise ClickException(f"Unable to create auth project: [{e.text}]")


def create_auth_policy(
    ac: AuthClient,
    project_id: str,
    display_name: str,
    description: str,
    include_domains: list[str] | MissingType,
    exclude_domains: list[str] | MissingType,
    high_assurance: bool | MissingType,
    timeout: int | MissingType,
    require_mfa: bool | MissingType,
) -> str:
    """
    Uses the Globus SDK to create an auth policy and returns
    the policy_id after creation
    """
    try:
        assert ac._app is not None  # mypy
        resp = gare_handler(
            ac._app.login,
            ac.create_policy,
            project_id=project_id,
            display_name=display_name,
            description=description,
            domain_constraints_include=include_domains,
            domain_constraints_exclude=exclude_domains,
            high_assurance=high_assurance,
            authentication_assurance_timeout=timeout,
            required_mfa=require_mfa,
        )
        return resp["policy"]["id"]
    except GlobusAPIError as e:
        raise ClickException(f"Unable to create authentication policy: [{e.text}]")


@app.command(
    "enable-on-boot",
    help="Enable on-boot persistence; restarts the endpoint when the machine restarts.",
)
@name_or_uuid_arg
def enable_on_boot_cmd(ep_dir: pathlib.Path):
    app = get_globus_app_with_scopes()
    enable_on_boot(ep_dir, app=app)


@app.command(
    "disable-on-boot",
    help="Disable on-boot persistence.",
)
@name_or_uuid_arg
def disable_on_boot_cmd(ep_dir: pathlib.Path):
    disable_on_boot(ep_dir)


RENDER_USER_CONFIG_EPILOG = """
Example invocations:

  $ {prog} render-user-config -t my_template.yaml.j2 -s user_config_schema.json -o user_options.json

  $ cat user_options.json | {prog} render-user-config -e my_endpoint -o -
"""  # noqa: E501


@app.command(
    "render-user-config",
    epilog=RENDER_USER_CONFIG_EPILOG.format(prog=sys.argv[0].rsplit("/", 1)[1]),
    context_settings={"max_content_width": shutil.get_terminal_size().columns},
)
@click.option(
    "--endpoint",
    "-e",
    callback=get_ep_dir_by_name_or_uuid,
    help="Endpoint name or UUID to read template and files from",
    expose_value=False,
)
@click.option(
    "--template",
    "-t",
    type=click.File(),
    help="YAML Jinja2 template to render",
)
@click.option(
    "--user-options",
    "-o",
    type=click.File(),
    help="JSON user options to render to the template",
)
@click.option(
    "--user-schema",  # shorter than --user-config-schema to fit Click's help columns
    "-s",
    type=click.File(),
    help="JSON schema to validate user options against",
)
@optgroup.group(
    "Reserved Variables",
    help="Users cannot set these normally; they are provided for admin convenience",
)
@optgroup.option(
    "--parent-config",
    type=click.File(),
    help="YAML data to render to the 'parent_config' reserved variable",
)
@optgroup.option(
    "--user-runtime",
    type=click.File(),
    help="JSON data to render to the 'user_runtime' reserved variable",
)
@optgroup.option(
    "--mapped-identity",
    type=click.File(),
    help="JSON data to render to the 'mapped_identity' reserved variable",
)
def render_user_config(
    ep_dir: pathlib.Path | None,
    template: t.TextIO | None,
    user_options: t.TextIO | None,
    user_schema: t.TextIO | None,
    parent_config: t.TextIO | None,
    user_runtime: t.TextIO | None,
    mapped_identity: t.TextIO | None,
):
    """
    Render a user config template to stdout

    When the --endpoint option is supplied, any files used in rendering are pulled from
    that endpoint's config by default, but can still be manually overridden as needed.
    Otherwise, any files of interest must be specified via the other options.

    All file options also interpret the hyphen (-) as a switch to read from stdin.
    """

    _do_render_user_config(
        parent_ep_dir=ep_dir,
        template_file=template,
        user_options_file=user_options,
        user_schema_file=user_schema,
        parent_config_file=parent_config,
        user_runtime_file=user_runtime,
        mapped_identity_file=mapped_identity,
    )


def _do_render_user_config(
    parent_ep_dir: pathlib.Path | None,
    template_file: t.TextIO | None,
    user_options_file: t.TextIO | None,
    user_schema_file: t.TextIO | None,
    parent_config_file: t.TextIO | None,
    user_runtime_file: t.TextIO | None,
    mapped_identity_file: t.TextIO | None,
):
    stdin_files = [
        f
        for f in (
            template_file,
            user_options_file,
            user_schema_file,
            parent_config_file,
            user_runtime_file,
            mapped_identity_file,
        )
        if f == sys.stdin
    ]
    if len(stdin_files) > 1:
        raise ClickException(
            "At most one input may be read from stdin across all options."
        )

    if parent_ep_dir is None and template_file is None:
        raise ClickException("Must provide at least one of --endpoint or --template.")

    parent_config = ManagerEndpointConfig()
    user_config_template: str
    user_config_template_path: pathlib.Path
    user_config_schema = {}

    if parent_ep_dir is not None:
        parent_config = get_config(parent_ep_dir)  # type: ignore[assignment]
        if not isinstance(parent_config, ManagerEndpointConfig):
            raise ClickException(
                f"Endpoint {parent_ep_dir.name} does not support templating."
            )

        user_config_template_path = (
            parent_config.user_config_template_path
            or Endpoint.user_config_template_path(parent_ep_dir)
        )
        user_config_template = load_user_config_template(user_config_template_path)
        _user_config_schema_path = (
            parent_config.user_config_schema_path
            or Endpoint.user_config_schema_path(parent_ep_dir)
        )
        user_config_schema = load_user_config_schema(_user_config_schema_path) or {}

    if parent_config_file is not None:
        try:
            parent_config = load_config_yaml(
                parent_config_file.read()
            )  # type: ignore[assignment]
        except Exception as e:
            raise ClickExceptionWithContext(
                f"Invalid parent config data in {parent_config_file.name}."
            ) from e
        if not isinstance(parent_config, ManagerEndpointConfig):
            raise ClickException("Provided parent config does not support templating.")

    if template_file is not None:
        user_config_template = template_file.read()
        user_config_template_path = pathlib.Path(template_file.name)

    def _read_json_dict(file: t.TextIO, description: str) -> dict[str, t.Any]:
        try:
            data = json.load(file)
        except json.JSONDecodeError as e:
            raise ClickExceptionWithContext(
                f"Invalid JSON in {file.name} for {description}."
            ) from e
        if not isinstance(data, dict):
            raise ClickException(
                f"Invalid {description} data in {file.name}: must be a JSON dictionary."
            )
        return data

    if user_schema_file is not None:
        user_config_schema = _read_json_dict(user_schema_file, "user schema")

    if user_options_file is not None:
        user_opts = _read_json_dict(user_options_file, "user options")
    else:
        user_opts = {}

    if user_runtime_file is not None:
        user_runtime = _read_json_dict(user_runtime_file, "user runtime")
    else:
        user_runtime = asdict(create_user_runtime())
        # sanitization logic in render command expects only jsonable values
        user_runtime = json.loads(json.dumps(user_runtime))

    if mapped_identity_file is not None:
        mapped_identity = MappedPosixIdentity(
            **_read_json_dict(mapped_identity_file, "mapped identity")
        )
    else:
        mapped_identity = MappedPosixIdentity(
            local_user_record=pwd.getpwuid(os.getuid()),
            matched_identity=uuid.UUID(int=0),
            globus_identity_candidates=[],  # not used for rendering
        )

    try:
        rendered_config = render_config_user_template(
            parent_config=parent_config,
            user_config_template=user_config_template,
            user_config_template_path=user_config_template_path,
            mapped_identity=mapped_identity,
            user_config_schema=user_config_schema,
            user_opts=user_opts,
            user_runtime=user_runtime,
        )
    except Exception as e:
        raise ClickExceptionWithContext(
            "Failed to render user configuration template."
        ) from e

    log.info("Rendered user config:")
    print(rendered_config)


@app.command(
    "migrate-to-template-capable",
    help="Add configuration templating to an existing endpoint",
)
@click.option(
    "--yes",
    is_flag=True,
    default=False,
    help="Do not ask for confirmation to migrate the endpoint.",
)
@name_or_uuid_arg
def migrate_to_template_capable(ep_dir: pathlib.Path, yes: bool):
    """Migrate an endpoint to be template capable"""
    if not yes:
        click.confirm(
            f"> Are you sure you want to migrate endpoint '{ep_dir.name}' to be"
            " template capable? This is a one-way operation.",
            abort=True,
        )

    try:
        Endpoint.migrate_to_template_capable(ep_dir)
    except Exception as e:
        raise ClickException(f"Failed to migrate endpoint: {e}")
    else:
        log.info(f"Endpoint {ep_dir.name} successfully migrated to template capable.")


def cli_run():
    """Entry point for setuptools to point to"""
    app()


if __name__ == "__main__":
    app()
