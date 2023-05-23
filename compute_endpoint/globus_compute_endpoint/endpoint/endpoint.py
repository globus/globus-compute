from __future__ import annotations

import json
import logging
import os
import pathlib
import pwd
import re
import shutil
import signal
import socket
import sys
import typing
import uuid
from string import Template

import daemon
import daemon.pidfile
import psutil
import setproctitle
import texttable
from globus_compute_endpoint import __version__
from globus_compute_endpoint.endpoint import default_config as endpoint_default_config
from globus_compute_endpoint.endpoint.interchange import EndpointInterchange
from globus_compute_endpoint.endpoint.result_store import ResultStore
from globus_compute_endpoint.endpoint.utils import _redact_url_creds
from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.logging_config import setup_logging
from globus_compute_sdk.sdk.client import Client
from globus_sdk import GlobusAPIError, NetworkError

log = logging.getLogger(__name__)


class Endpoint:
    """
    Endpoint is primarily responsible for configuring, launching and stopping
    the Endpoint.
    """

    def __init__(self, debug=False):
        """Initialize the Endpoint

        Parameters
        ----------
        debug: Bool
            Enable debug logging. Default: False
        """
        self.debug = debug

    @staticmethod
    def _config_file_path(endpoint_dir: pathlib.Path) -> pathlib.Path:
        return endpoint_dir / "config.py"

    @staticmethod
    def update_config_file(
        ep_name: str,
        original_path: pathlib.Path,
        target_path: pathlib.Path,
        multi_tenant: bool,
        display_name: str | None,
    ):
        endpoint_config = Template(original_path.read_text()).substitute(name=ep_name)

        # Note that this logic does NOT support replacing arbitrary
        # display_name="abc xyz" entries, as str quote parsing is beyond the
        # current use case of calling this method only at endpoint creation
        display_str = "None" if display_name is None else f'"{display_name}"'
        display_text = f"display_name={display_str},"

        d_index = endpoint_config.find("display_name")
        if d_index != -1:
            if endpoint_config[d_index + 13 : d_index + 18] == "None,":
                if display_name is not None:
                    endpoint_config = endpoint_config.replace(
                        "display_name=None,", display_text
                    )
            else:
                raise Exception(
                    "Modifying existing display_name not supported "
                    f"at this time. Please edit {original_path} manually."
                )
        else:
            endpoint_config = endpoint_config.replace(
                "\nconfig = Config(\n", f"\nconfig = Config(\n    {display_text}\n"
            )

        if endpoint_config.find("multi_tenant=") != -1:
            # If the option is already in the config, merely update the value
            endpoint_config = re.sub(
                "multi_tenant=(True|False)",
                f"multi_tenant={multi_tenant is True}",
                endpoint_config,
            )
        elif multi_tenant:
            # If the option isn't pre-existing, add only if set to True
            endpoint_config = endpoint_config.replace(
                "\nconfig = Config(\n", "\nconfig = Config(\n    multi_tenant=True,\n"
            )

        target_path.write_text(endpoint_config)

    @staticmethod
    def init_endpoint_dir(
        endpoint_dir: pathlib.Path,
        endpoint_config: pathlib.Path | None = None,
        multi_tenant=False,
        display_name: str | None = None,
    ):
        """Initialize a clean endpoint dir

        :param endpoint_dir pathlib.Path Path to the endpoint configuration dir
        :param endpoint_config str Path to a config file to be used instead
         of the Globus Compute default config file
        :param multi_tenant bool Whether the endpoint is a multi-user endpoint
        :param display_name str A display name to use, if desired
        """
        log.debug(f"Creating endpoint dir {endpoint_dir}")
        user_umask = os.umask(0o0077)
        os.umask(0o0077 | (user_umask & 0o0400))  # honor only the UR bit for dirs
        try:
            # pathlib.Path does not handle unusual umasks (e.g., 0o0111) so well
            # in the parents=True case, so temporarily change it.  This is nominally
            # only an issue for totally new users (no .globus_compute/!), but that is
            # also precisely the interaction -- the first one -- that should go smoothly
            endpoint_dir.mkdir(parents=True, exist_ok=True)

            config_target_path = Endpoint._config_file_path(endpoint_dir)

            if endpoint_config is None:
                endpoint_config = pathlib.Path(endpoint_default_config.__file__)

            Endpoint.update_config_file(
                endpoint_dir.name,
                endpoint_config,
                config_target_path,
                multi_tenant,
                display_name,
            )
        finally:
            os.umask(user_umask)

    @staticmethod
    def configure_endpoint(
        conf_dir: pathlib.Path,
        endpoint_config: str | None,
        multi_tenant: bool = False,
        display_name: str | None = None,
    ):
        ep_name = conf_dir.name
        if conf_dir.exists():
            print(conf_dir)
            print(f"config dir <{ep_name}> already exists")
            raise Exception("ConfigExists")

        templ_conf_path = pathlib.Path(endpoint_config) if endpoint_config else None
        Endpoint.init_endpoint_dir(
            conf_dir, templ_conf_path, multi_tenant, display_name
        )
        config_path = Endpoint._config_file_path(conf_dir)
        if multi_tenant:
            print(f"Created multi-tenant profile for <{ep_name}>")
        else:
            print(f"Created profile for <{ep_name}>")
        print(
            f"\n    Configuration file: {config_path}\n"
            "\nUse the `start` subcommand to run it:\n"
            f"\n    $ globus-compute-endpoint start {ep_name}"
        )

    @staticmethod
    def validate_endpoint_name(path_name: str) -> None:
        # Validate the path by removing all relative path modifiers (i.e., ../),
        # taking the basename, ensuring it's not Unix hidden (initial dot [.]), and
        # has no whitespace.  The path name is used deep within Parsl, which uses
        # raw shell to perform some actions ... and is not robust about it at all.
        # The path of least resistence, then, is to restrict this EP name upfront.
        # Examples:
        #
        # Good: "nice_normal_name"
        # Good: "91239anothervalidname"
        # Bad: " has_a_space"
        # Bad: "has_a_space "
        # Bad: "has a_space"
        # Bad: "has/../relative_path_syntax"
        # Bad: ".has_an_initial_dot(.);_'hidden'_directories_disallowed"
        # Bad: "a" * 129  # too long
        # Bad: ""  # come on now
        # Bad: "/absolute_path"
        # Bad: "contains...\r\v\t\n...other_whitespace"
        # Bad: "we_\\_do_not_accept_escapes"
        # Bad: "we_don't_accept_single_qoutes"
        # Bad: 'we_do_not_accept_"double_qoutes"'
        err_msg = "Invalid endpoint name: "
        pathname_re = re.compile(r"[\\/\s'\"]")
        if not path_name:
            err_msg += "Received no endpoint name"
            raise ValueError(err_msg)

        if len(path_name) > 128:
            err_msg += f"must be less than 129 characters (length: {len(path_name)})"
            raise ValueError(err_msg)

        # let pathlib resolve to an absolute (no ../), then take basename
        validated = pathlib.Path(path_name).resolve().name
        if validated != path_name:
            err_msg += (
                "no '..' or '/' characters"
                f"\n  Requested:  {path_name}"
                f"\n  Reduced to: {validated} (*potentially* valid)"
            )
            raise ValueError(err_msg)

        validated = pathname_re.sub("", validated).lstrip(".")
        if validated != path_name:
            err_msg += (
                "no whitespace (spaces, newlines, tabs), slashes, or prefixed '.'"
                f"\n  Requested:  {path_name}"
                f"\n  Reduced to: {validated} (*potentially* valid)"
            )
            raise ValueError(err_msg)

        # verify it's still a valid pathname *after* our ministrations
        validated = pathlib.Path(validated).name

        # only valid if completely unchanged by pathlib and basename shenanigans
        if path_name != validated:
            err_msg += (
                "unknown reason"
                f"\n  Requested:  {path_name}"
                f"\n  Reduced to: {validated} (*potentially* valid)"
            )
            raise ValueError(err_msg)

    @staticmethod
    def get_endpoint_id(endpoint_dir: pathlib.Path) -> str | None:
        info_file_path = endpoint_dir / "endpoint.json"
        try:
            return json.loads(info_file_path.read_bytes())["endpoint_id"]
        except FileNotFoundError:
            pass
        return None

    @staticmethod
    def get_or_create_endpoint_uuid(
        endpoint_dir: pathlib.Path, endpoint_uuid: str | None
    ) -> str:
        ep_id = Endpoint.get_endpoint_id(endpoint_dir)
        if not ep_id:
            ep_id = endpoint_uuid or str(uuid.uuid4())
        return ep_id

    @staticmethod
    def get_funcx_client(config: Config | None) -> Client:
        if config:
            funcx_client_options = {
                "funcx_service_address": config.funcx_service_address,
                "environment": config.environment,
            }

            return Client(**funcx_client_options)
        else:
            return Client()

    def start_endpoint(
        self,
        endpoint_dir,
        endpoint_uuid,
        endpoint_config: Config,
        log_to_console: bool,
        no_color: bool,
        reg_info: dict,
        die_with_parent: bool = False,
    ):
        # This is to ensure that at least 1 executor is defined
        if not endpoint_config.executors:
            raise Exception(
                f"Endpoint config file at {endpoint_dir} is missing "
                "executor definitions"
            )

        pid_check = Endpoint.check_pidfile(endpoint_dir)
        # if the pidfile exists, we should return early because we don't
        # want to attempt to create a new daemon when one is already
        # potentially running with the existing pidfile
        if pid_check["exists"]:
            if pid_check["active"]:
                log.info("Endpoint is already active")
                sys.exit(-1)
            else:
                log.info(
                    "A prior Endpoint instance appears to have been terminated without "
                    "proper cleanup. Cleaning up now."
                )
                Endpoint.pidfile_cleanup(endpoint_dir)

        result_store = ResultStore(endpoint_dir=endpoint_dir)

        # Create a daemon context
        # If we are running a full detached daemon then we will send the output to
        # log files, otherwise we can piggy back on our stdout
        if endpoint_config.detach_endpoint:
            stdout: typing.TextIO = open(
                os.path.join(endpoint_dir, endpoint_config.stdout), "a+"
            )
            stderr: typing.TextIO = open(
                os.path.join(endpoint_dir, endpoint_config.stderr), "a+"
            )
        else:
            stdout = sys.stdout
            stderr = sys.stderr

        try:
            pid_file = endpoint_dir / "daemon.pid"
            context = daemon.DaemonContext(
                working_directory=endpoint_dir,
                umask=0o002,
                pidfile=daemon.pidfile.PIDLockFile(pid_file),
                stdout=stdout,
                stderr=stderr,
                detach_process=endpoint_config.detach_endpoint,
            )

        except Exception:
            log.exception(
                "Caught exception while trying to setup endpoint context dirs"
            )
            exit(-1)

        # place registration after everything else so that the endpoint will
        # only be registered if everything else has been set up successfully
        if not reg_info:
            endpoint_uuid = Endpoint.get_or_create_endpoint_uuid(
                endpoint_dir, endpoint_uuid
            )
            log.debug("Attempting registration; trying with eid: %s", endpoint_uuid)
            try:
                fx_client = Endpoint.get_funcx_client(endpoint_config)
                reg_info = fx_client.register_endpoint(
                    endpoint_dir.name,
                    endpoint_uuid,
                    metadata=Endpoint.get_metadata(endpoint_config),
                    multi_tenant=False,
                    display_name=endpoint_config.display_name,
                )

            except GlobusAPIError as e:
                if e.http_status in (409, 410, 423):
                    # CONFLICT, GONE or LOCKED
                    log.warning(f"Endpoint registration blocked.  [{e.message}]")
                    exit(os.EX_UNAVAILABLE)
                raise

            except NetworkError as e:
                # the output of a NetworkError exception is huge and unhelpful, so
                # it seems better to just stringify it here and get a concise error
                log.exception(
                    f"Caught exception while attempting endpoint registration: {e}"
                )
                log.critical(
                    "globus-compute-endpoint is unable to reach the Globus Compute "
                    "service due to a NetworkError \n"
                    "Please make sure that the Globus Compute service address you "
                    "provided is reachable \n"
                    "and then attempt restarting the endpoint"
                )
                exit(os.EX_TEMPFAIL)

            ret_ep_uuid = reg_info.get("endpoint_id")
            if ret_ep_uuid != endpoint_uuid:
                log.error(
                    "Unexpected response from server: mismatched endpoint id."
                    f"\n  Expected: {endpoint_uuid}, received: {ret_ep_uuid}"
                )
                exit(os.EX_SOFTWARE)

        try:
            endpoint_uuid = reg_info["endpoint_id"]
        except KeyError:
            log.error("Invalid credential structure")
            exit(os.EX_DATAERR)

        # sanitize passwords in logs
        log_reg_info = _redact_url_creds(repr(reg_info))
        log.debug(f"Registration information: {log_reg_info}")

        json_file = endpoint_dir / "endpoint.json"

        # `endpoint_id` key kept for backward compatibility when
        # globus-compute-endpoint list is called
        ep_info = {"endpoint_id": endpoint_uuid}
        json_file.write_text(json.dumps(ep_info))
        log.debug(f"Registration info written to {json_file}")

        ptitle = f"Globus Compute Endpoint ({endpoint_uuid}, {endpoint_dir.name})"
        if endpoint_config.environment:
            ptitle += f" - {endpoint_config.environment}"
        ptitle += f" [{setproctitle.getproctitle()}]"
        setproctitle.setproctitle(ptitle)

        parent_pid = 0
        if die_with_parent:
            parent_pid = os.getppid()

        log.info("Launching endpoint daemon process")

        # NOTE
        # It's important that this log is emitted before we enter the daemon context
        # because daemonization closes down everything, a log message inside the
        # context won't write the currently configured loggers
        logfile = endpoint_dir / "endpoint.log"
        log.info(
            "Reconfiguring logging for daemonization. logfile: %s , debug: %s",
            logfile,
            self.debug,
        )

        ostream = None
        if sys.stdout.isatty():
            ostream = sys.stdout
        elif sys.stderr.isatty():
            ostream = sys.stderr
        if ostream:
            msg = f"Starting endpoint; registered ID: {endpoint_uuid}"
            if log_to_console:
                # make more prominent against other drab gray text ...
                msg = f"\n    {msg}\n"
            print(msg, file=ostream)

        with context:
            # Per DaemonContext implementation, and that we _don't_ pass stdin,
            # fd 0 is already connected to devnull.  Unfortunately, there is an
            # as-yet unknown interaction on Polaris (ALCF) that needs this
            # connection setup *again*.  So, repeat what daemon context already
            # did, and dup2 stdin from devnull to ... devnull.  (!@#$%^&*)
            # On any other system, this should make no difference (same file!)
            with open(os.devnull) as nullf:
                if os.dup2(nullf.fileno(), 0) != 0:
                    raise Exception("Unable to close stdin")

            setup_logging(
                logfile=logfile,
                debug=self.debug,
                console_enabled=log_to_console,
                no_color=no_color,
            )

            Endpoint.daemon_launch(
                endpoint_uuid,
                endpoint_dir,
                endpoint_config,
                reg_info,
                result_store,
                parent_pid,
            )

    @staticmethod
    def daemon_launch(
        endpoint_uuid,
        endpoint_dir,
        endpoint_config: Config,
        reg_info,
        result_store: ResultStore,
        parent_pid: int,
    ):
        interchange = EndpointInterchange(
            config=endpoint_config,
            reg_info=reg_info,
            endpoint_id=endpoint_uuid,
            endpoint_dir=endpoint_dir,
            result_store=result_store,
            logdir=endpoint_dir,
            parent_pid=parent_pid,
        )

        interchange.start()

        log.critical("Interchange terminated.")

    @staticmethod
    def stop_endpoint(
        endpoint_dir: pathlib.Path,
        endpoint_config: Config | None,
        remote: bool = False,
    ):
        pid_path = endpoint_dir / "daemon.pid"
        ep_name = endpoint_dir.name

        if remote is True:
            endpoint_id = Endpoint.get_endpoint_id(endpoint_dir)
            if not endpoint_id:
                raise ValueError(f"Endpoint <{ep_name}> could not be located")

            fx_client = Endpoint.get_funcx_client(endpoint_config)
            fx_client.stop_endpoint(endpoint_id)

        ep_status = Endpoint.check_pidfile(endpoint_dir)
        if ep_status["exists"] and not ep_status["active"]:
            Endpoint.pidfile_cleanup(endpoint_dir)
            return
        elif not ep_status["exists"]:
            log.info(f"Endpoint <{ep_name}> is not active.")
            return

        log.debug(f"{ep_name} has a daemon.pid file")
        pid = int(pid_path.read_text().strip())
        try:
            log.debug(f"Signaling process: {pid}")
            # For all the processes, including the deamon and its descendants,
            # send SIGTERM, wait for 10s, and then SIGKILL any still alive.
            grace_period_s = 10
            parent = psutil.Process(pid)
            processes = parent.children(recursive=True)
            processes.append(parent)
            for p in processes:
                p.send_signal(signal.SIGTERM)
            terminated, alive = psutil.wait_procs(processes, timeout=grace_period_s)
            for p in alive:
                try:
                    p.send_signal(signal.SIGKILL)
                except psutil.NoSuchProcess:
                    pass

            if pid_path.exists():
                log.warning(f"Endpoint <{ep_name}> did not gracefully shutdown")
                # Do cleanup anyway, TODO figure out where it should have done that
                pid_path.unlink(missing_ok=True)
            else:
                log.info(f"Endpoint <{ep_name}> is now stopped")
        except OSError:
            log.warning(f"Endpoint <{ep_name}> could not be terminated")
            log.warning(f"Attempting Endpoint <{ep_name}> cleanup")
            pid_path.unlink(missing_ok=True)
            exit(-1)

    @staticmethod
    def delete_endpoint(
        endpoint_dir: pathlib.Path, endpoint_config: Config | None, force: bool = False
    ):
        ep_name = endpoint_dir.name

        if not endpoint_dir.exists():
            log.warning(f"Endpoint <{ep_name}> does not exist")
            exit(-1)

        endpoint_id = Endpoint.get_endpoint_id(endpoint_dir)
        if endpoint_id is None:
            log.warning(f"Endpoint <{ep_name}> could not be located")
            if not force:
                exit(-1)

        # Delete endpoint from web service
        try:
            fx_client = Endpoint.get_funcx_client(endpoint_config)
            fx_client.delete_endpoint(endpoint_id)
            log.info(f"Endpoint <{ep_name}> has been deleted from the web service")
        except GlobusAPIError as e:
            log.warning(
                f"Endpoint <{ep_name}> could not be deleted from the web service"
                f"  [{e.message}]"
            )
            if not force:
                log.critical("Exiting without deleting the endpoint")
                exit(os.EX_UNAVAILABLE)
        except NetworkError as e:
            log.warning(
                f"Endpoint <{ep_name}> could not be deleted from the web service"
                f"  [{e}]"
            )
            if not force:
                log.critical(
                    "globus-compute-endpoint is unable to reach the Globus Compute "
                    "service due to a NetworkError \n"
                    "Please make sure that the Globus Compute service address you "
                    "provided is reachable \n"
                    "and then attempt to delete the endpoint again"
                )
                exit(os.EX_TEMPFAIL)

        # Stop endpoint to handle process cleanup
        Endpoint.stop_endpoint(endpoint_dir, None)

        # Delete endpoint directory
        shutil.rmtree(endpoint_dir)

        log.info(f"Endpoint <{ep_name}> has been deleted.")

    @staticmethod
    def check_pidfile(endpoint_dir: pathlib.Path):
        """Helper function to identify possible dead endpoints

        Returns a record with 'exists' and 'active' fields indicating
        whether the pidfile exists, and whether the process is active if it does exist
        (The endpoint can only start correctly if there is no pidfile)

        Parameters
        ----------
        endpoint_dir : pathlib.Path
            Configuration directory of the endpoint
        """
        status = {"exists": False, "active": False}
        pid_path = endpoint_dir / "daemon.pid"
        if not pid_path.exists():
            return status

        status["exists"] = True

        try:
            pid = int(pid_path.read_text().strip())
            psutil.Process(pid)
            status["active"] = True
        except ValueError:
            # Invalid literal for int() parsing
            pass
        except psutil.NoSuchProcess:
            pass

        return status

    @staticmethod
    def pidfile_cleanup(endpoint_dir: pathlib.Path):
        (endpoint_dir / "daemon.pid").unlink(missing_ok=True)
        log.info(f"Endpoint <{endpoint_dir.name}> has been cleaned up.")

    @staticmethod
    def get_endpoints(funcx_conf_dir: pathlib.Path | str) -> dict[str, dict]:
        """
        Gets a dictionary that contains information about all locally
        known endpoints.

        "status" can be one of:
            ["Initialized", "Running", "Disconnected", "Stopped"]

        Example output:
        {
            "default": {
                 "status": "Running",
                 "id": "123abcde-a393-4456-8de5-123456789abc"
            },
            "my_test_ep": {
                 "status": "Disconnected",
                 "id": "xxxxxxxx-xxxx-1234-abcd-xxxxxxxxxxxx"
            }
        }
        """
        ep_statuses = {}

        funcx_conf_dir = pathlib.Path(funcx_conf_dir)
        ep_dir_paths = (p.parent for p in funcx_conf_dir.glob("*/config.py"))
        for ep_path in ep_dir_paths:
            ep_status = {
                "status": "Initialized",
                "id": Endpoint.get_endpoint_id(ep_path),
            }
            ep_statuses[ep_path.name] = ep_status
            if not ep_status["id"]:
                continue

            pid_check = Endpoint.check_pidfile(ep_path)
            if pid_check["active"]:
                ep_status["status"] = "Running"
            elif pid_check["exists"]:
                ep_status["status"] = "Disconnected"
            else:
                ep_status["status"] = "Stopped"

        return ep_statuses

    @staticmethod
    def get_running_endpoints(conf_dir: pathlib.Path):
        return {
            ep_name: ep_status
            for ep_name, ep_status in Endpoint.get_endpoints(conf_dir).items()
            if ep_status["status"].lower() == "running"
        }

    @staticmethod
    def print_endpoint_table(conf_dir: pathlib.Path, ofile=None):
        """
        Converts locally configured endpoint list to a text based table
        and prints the output.
          For example format, see the texttable module
        """
        if not ofile:
            ofile = sys.stdout

        endpoints = Endpoint.get_endpoints(conf_dir)
        if not endpoints:
            print(
                "No endpoints configured!\n\n "
                "(Hint: globus-compute-endpoint configure)",
                file=ofile,
            )
            return

        table = texttable.Texttable()
        headings = ["Endpoint ID", "Status", "Endpoint Name"]
        table.header(headings)

        idx_id = 0
        idx_st = 1
        idx_name = 2
        width_st = len(headings[idx_st])
        width_id = len(headings[idx_id])
        for ep_name, ep_info in endpoints.items():
            table.add_row([ep_info["id"], ep_info["status"], ep_name])
            width_id = max(width_id, len(str(ep_info["id"])))
            width_st = max(width_st, len(str(ep_info["status"])))

        tsize = shutil.get_terminal_size()
        max_width = max(68, tsize.columns)  # require at least a reasonable size ...
        table.set_max_width(max_width)  # ... but allow to expand to terminal width

        # trickery here, but don't want to subclass texttable -- a very stable
        # library -- at this time; ensure that the only "fungible" column is the
        # name.  Can redress ~when~ *if* this becomes a problem.  "Simple"
        table._compute_cols_width()
        if table._width[idx_id] < width_id:
            table._width[idx_name] -= width_id - table._width[idx_id]
            table._width[idx_id] = width_id
        if table._width[idx_st] < width_st:
            table._width[idx_name] -= width_st - table._width[idx_st]
            table._width[idx_st] = width_st

        # ensure no mistakes -- width of name (well, _any_) column can't be < 0
        table._width[idx_name] = max(10, table._width[idx_name])

        print(table.draw(), file=ofile)

    @staticmethod
    def get_metadata(config: Config) -> dict:
        metadata: dict = {}

        metadata["endpoint_version"] = __version__
        metadata["hostname"] = socket.getfqdn()

        # should be more accurate than `getpass.getuser()` in non-login situations
        metadata["local_user"] = pwd.getpwuid(os.getuid()).pw_name

        # the following are read from the HTTP request by the web service, but can be
        # overridden here if desired:
        metadata["ip_address"] = None
        metadata["sdk_version"] = None

        try:
            metadata["config"] = _serialize_config(config)
        except Exception as e:
            log.warning(
                f"Error when serializing config ({type(e).__name__}). Ignoring."
            )
            log.debug("Config serialization exception details", exc_info=e)

        return metadata


def _serialize_config(config: Config) -> dict:
    """
    Short-term serialization method until config.py is replaced with config.yaml
    """

    expand_list = ["strategy", "provider", "launcher"]
    pass_through_list = ["allowed_functions"]

    def to_dict(o):
        mems = {"_type": type(o).__name__}

        for k, v in o.__dict__.items():
            if k.startswith("_"):
                continue

            if k in expand_list:
                mems[k] = to_dict(v)
            elif isinstance(v, str) or k in pass_through_list:
                mems[k] = v
            else:
                mems[k] = repr(v)

        return mems

    result = to_dict(config)
    # when we move to config.yaml, should only need to support a single executor
    result["executors"] = [to_dict(executor) for executor in config.executors]

    return result
