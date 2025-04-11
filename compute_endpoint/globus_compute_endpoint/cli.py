from __future__ import annotations

import json
import logging
import os
import pathlib
import sys
import textwrap
import uuid

import click
from click import ClickException
from click_option_group import optgroup
from globus_compute_endpoint.boot_persistence import disable_on_boot, enable_on_boot
from globus_compute_endpoint.endpoint.config import (
    ManagerEndpointConfig,
    UserEndpointConfig,
)
from globus_compute_endpoint.endpoint.config.utils import get_config, load_config_yaml
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_endpoint.endpoint.utils import (
    send_endpoint_startup_failure_to_amqp,
    user_input_select,
)
from globus_compute_endpoint.exception_handling import handle_auth_errors
from globus_compute_endpoint.logging_config import setup_logging
from globus_compute_sdk.sdk.auth.auth_client import ComputeAuthClient
from globus_compute_sdk.sdk.auth.globus_app import get_globus_app
from globus_compute_sdk.sdk.auth.whoami import print_whoami_info
from globus_compute_sdk.sdk.compute_dir import ensure_compute_dir
from globus_compute_sdk.sdk.diagnostic import do_diagnostic_base
from globus_compute_sdk.sdk.web_client import WebClient
from globus_sdk import MISSING, AuthClient, GlobusAPIError, GlobusApp, MissingType

try:
    from globus_compute_endpoint.endpoint.endpoint_manager import EndpointManager
except ImportError as _e:
    _has_multi_user = False
    if "--debug" in sys.argv and sys.stderr.isatty():
        # We haven't set up logging yet so manually print for now
        print(f"(DEBUG) Failed to import from file: {__file__}", file=sys.stderr)
        print(f"(DEBUG) [{type(_e).__name__}] {_e}", file=sys.stderr)
else:
    _has_multi_user = True

log = logging.getLogger(__name__)


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


def get_globus_app_with_scopes() -> GlobusApp:
    try:
        app = get_globus_app()
    except (RuntimeError, ValueError) as e:
        raise ClickException(str(e))
    app.add_scope_requirements(
        {
            WebClient.scopes.resource_server: WebClient.default_scope_requirements,
            ComputeAuthClient.scopes.resource_server: ComputeAuthClient.default_scope_requirements,  # noqa E501
        }
    )
    return app


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

        msg = textwrap.dedent(
            f"""
            There is no endpoint configuration on this machine {ep_info}

            1. Please create a configuration template with:
                globus-compute-endpoint configure {ep_name}
            2. Update the configuration
            3. Start the endpoint
            """
        )
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
@click.option("--endpoint-config", default=None, help="a template config to use")
@click.option(
    "--multi-user",
    is_flag=True,
    default=False,
    help="Configure endpoint as multi-user capable",
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
    help=(
        "How old (in seconds) a login session can be and still be compliant. "
        "If set, the auth policy will be High Assurance(HA)"
    ),
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
    multi_user: bool,
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
    try:
        Endpoint.validate_endpoint_name(name)
    except ValueError as e:
        raise ClickException(str(e))

    if multi_user and not _has_multi_user:
        raise ClickException("multi-user endpoints are not supported on this system")

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
            timeout=auth_timeout or MISSING,
            require_mfa=auth_policy_mfa_required or MISSING,
        )

    compute_dir = get_config_dir()
    ep_dir = compute_dir / name
    Endpoint.configure_endpoint(
        ep_dir,
        endpoint_config,
        multi_user,
        high_assurance,
        display_name,
        auth_policy,
        subscription_id,
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

    try:
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

    except (SystemExit, Exception) as e:
        if isinstance(e, SystemExit):
            if e.code in (0, None):
                # normal, system exit
                raise

        if reg_info:
            # We're quitting anyway, so just let any exceptions bubble
            msg = f"Failed to start or unexpected error:\n  ({type(e).__name__}) {e}"
            send_endpoint_startup_failure_to_amqp(reg_info, msg=msg)

        raise


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


@app.command("self-diagnostic")
def self_diagnostic():
    """
    Note that this functionality has been migrated to the SDK, leaving only
    a redirect here.
    """
    print(
        "This endpoint self-diagnostic command has been deprecated.\n"
        "  Please use the `globus-compute-diagnostic` command from the SDK instead.",
        file=sys.stderr,
    )
    do_diagnostic_base(sys.argv[2:])


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
    projects = ac.get_projects()
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
        resp = ac.create_project(
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
    timeout: int | MissingType,
    require_mfa: bool | MissingType,
) -> str:
    """
    Uses the Globus SDK to create an auth policy and returns
    the policy_id after creation
    """

    try:
        resp = ac.create_policy(
            project_id=project_id,
            display_name=display_name,
            description=description,
            domain_constraints_include=include_domains,
            domain_constraints_exclude=exclude_domains,
            high_assurance=bool(timeout),
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


def cli_run():
    """Entry point for setuptools to point to"""
    app()


if __name__ == "__main__":
    app()
