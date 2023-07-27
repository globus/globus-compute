from __future__ import annotations

import fcntl
import json
import logging
import os
import pathlib
import pwd
import queue
import re
import resource
import signal
import socket
import sys
import threading
import time
import typing as t
from datetime import datetime

import globus_compute_sdk as GC
import setproctitle
import yaml
from globus_compute_endpoint import __version__
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.endpoint.config.utils import (
    render_config_user_template,
    serialize_config,
)
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_endpoint.endpoint.rabbit_mq.command_queue_subscriber import (
    CommandQueueSubscriber,
)
from globus_compute_endpoint.endpoint.utils import _redact_url_creds
from globus_sdk import GlobusAPIError, NetworkError

if t.TYPE_CHECKING:
    from pika.spec import BasicProperties


log = logging.getLogger(__name__)


class InvalidCommandError(Exception):
    pass


class EndpointManager:
    def __init__(
        self,
        conf_dir: pathlib.Path,
        endpoint_uuid: str | None,
        config: Config,
    ):
        log.info("Endpoint Manager initialization")

        self.conf_dir = conf_dir
        self._reload_requested = False
        self._time_to_stop = False
        self._kill_event = threading.Event()

        self._child_args: dict[int, tuple[int, int, str, str]] = {}
        self._wait_for_child = False

        self._command_queue: queue.SimpleQueue[
            tuple[int, BasicProperties, bytes]
        ] = queue.SimpleQueue()
        self._command_stop_event = threading.Event()

        endpoint_uuid = Endpoint.get_or_create_endpoint_uuid(conf_dir, endpoint_uuid)

        try:
            client_options = {
                "funcx_service_address": config.funcx_service_address,
                "environment": config.environment,
            }

            gcc = GC.Client(**client_options)
            reg_info = gcc.register_endpoint(
                conf_dir.name,
                endpoint_uuid,
                metadata=EndpointManager.get_metadata(config),
                multi_tenant=True,
            )
        except GlobusAPIError as e:
            if e.http_status == 409 or e.http_status == 423:
                # RESOURCE_CONFLICT or RESOURCE_LOCKED
                blocked_msg = f"Endpoint registration blocked.  [{e.text}]"
                print(blocked_msg)
                log.warning(blocked_msg)
                exit(os.EX_UNAVAILABLE)
            raise
        except NetworkError as e:
            log.exception("Network error while registering multi-tenant endpoint")
            log.critical(f"Network failure; unable to register endpoint: {e}")
            exit(os.EX_TEMPFAIL)

        upstream_ep_uuid = reg_info.get("endpoint_id")
        if upstream_ep_uuid != endpoint_uuid:
            log.error(
                "Unexpected response from server: mismatched endpoint id."
                f"\n  Expected: {endpoint_uuid}, received: {upstream_ep_uuid}"
            )
            exit(os.EX_SOFTWARE)

        self._endpoint_uuid_str = upstream_ep_uuid

        try:
            cq_info = reg_info["command_queue_info"]
            _ = cq_info["connection_url"], cq_info["queue"]
        except Exception as e:
            log.debug("%s", reg_info)
            log.error(
                "Invalid or unexpected registration data structure:"
                f" ({e.__class__.__name__}) {e}"
            )
            exit(os.EX_DATAERR)

        # sanitize passwords in logs
        log_reg_info = re.subn(r"://.*?@", r"://***:***@", repr(reg_info))
        log.debug(f"Registration information: {log_reg_info}")

        json_file = conf_dir / "endpoint.json"

        # `endpoint_id` key kept for backward compatibility when
        # globus-compute-endpoint list is called
        ep_info = {"endpoint_id": endpoint_uuid}
        json_file.write_text(json.dumps(ep_info))
        log.debug(f"Registration info written to {json_file}")

        # * == "multi-tenant"; not important until it is, so let it be subtle
        ptitle = f"Globus Compute Endpoint *({endpoint_uuid}, {conf_dir.name})"
        if config.environment:
            ptitle += f" - {config.environment}"
        ptitle += f" [{setproctitle.getproctitle()}]"
        setproctitle.setproctitle(ptitle)

        self._command = CommandQueueSubscriber(
            queue_info=cq_info,
            command_queue=self._command_queue,
            stop_event=self._command_stop_event,
            thread_name="CQS",
        )

    @staticmethod
    def get_metadata(config: Config) -> dict:
        # Piecemeal Config settings because for MT, most of the ST items are
        # unrelated -- the MT (aka EndpointManager) does not execute tasks
        return {
            "endpoint_version": __version__,
            "hostname": socket.getfqdn(),
            "local_user": pwd.getpwuid(os.getuid()).pw_name,
            "config": serialize_config(config),
        }

    def request_shutdown(self, sig_num, curr_stack_frame):
        self._time_to_stop = True

    def set_child_died(self, sig_num, curr_stack_fframe):
        self._wait_for_child = True

    def wait_for_children(self):
        try:
            self._wait_for_child = False
            wait_flags = os.WNOHANG
            pid, exit_status_ind = os.waitpid(-1, wait_flags)
            while pid > 0:
                try:
                    rc = os.waitstatus_to_exitcode(exit_status_ind)
                except ValueError:
                    rc = -127  # invalid signal number

                *_, proc_args = self._child_args.pop(pid, (None, None, None, None))
                proc_args = f" [{proc_args}]" if proc_args else ""
                if not rc:
                    log.info(f"Command stopped normally ({pid}){proc_args}")
                elif rc > 0:
                    log.warning(f"Command return code: {rc} ({pid}){proc_args}")
                elif rc == -127:
                    log.warning(f"Command unknown return code: ({pid}){proc_args}")
                else:
                    log.warning(
                        f"Command terminated by signal: {-rc} ({pid}){proc_args}"
                    )
                pid, exit_status_ind = os.waitpid(-1, wait_flags)

        except ChildProcessError:
            pass
        except Exception as e:
            log.exception(f"Failed to wait for a child process: {e}")

    def _install_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.request_shutdown)
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGQUIT, self.request_shutdown)

        signal.signal(signal.SIGCHLD, self.set_child_died)

    def start(self):
        log.info(f"\n\n========== Endpoint Manager begins: {self._endpoint_uuid_str}")

        msg_out = None
        if sys.stdout.isatty():
            msg_out = sys.stdout
        elif sys.stderr.isatty():
            msg_out = sys.stderr

        if msg_out:
            hl, r = "\033[104m", "\033[m"
            pld = f"{hl}{self._endpoint_uuid_str}{r}"
            print(f"        >>> Multi-Tenant Endpoint ID: {pld} <<<", file=msg_out)

        self._install_signal_handlers()

        try:
            self._event_loop()
        except Exception:
            log.exception("Unhandled exception; shutting down endpoint master")

        ptitle = f"[shutdown in progress] {setproctitle.getproctitle()}"
        setproctitle.setproctitle(ptitle)
        self._command_stop_event.set()
        self._kill_event.set()

        os.killpg(os.getpgid(0), signal.SIGTERM)

        proc_uid, proc_gid = os.getuid(), os.getgid()
        for msg_prefix, signum in (
            ("Signaling shutdown", signal.SIGTERM),
            ("Forcibly killing", signal.SIGKILL),
        ):
            for pid, (uid, gid, uname, proc_args) in list(self._child_args.items()):
                proc_ident = f"PID: {pid}, UID: {uid}, GID: {gid}, User: {uname}"
                log.info(f"{msg_prefix} of user endpoint ({proc_ident}) [{proc_args}]")
                try:
                    os.setresgid(gid, gid, -1)
                    os.setresuid(uid, uid, -1)
                    os.killpg(os.getpgid(pid), signum)
                except Exception as e:
                    log.warning(
                        f"User endpoint signal failed: {e} ({proc_ident}) [{proc_args}]"
                    )
                finally:
                    os.setresuid(proc_uid, proc_uid, -1)
                    os.setresgid(proc_gid, proc_gid, -1)

            deadline = time.time() + 10
            while self._child_args and time.time() < deadline:
                time.sleep(0.5)
                self.wait_for_children()

        self._command.join(5)
        log.info(
            "Shutdown complete."
            f"\n---------- Endpoint Manager ends: {self._endpoint_uuid_str}\n\n"
        )

    def _event_loop(self):
        self._command.start()

        local_user_lookup = {}
        try:
            with open("local_user_lookup.json") as f:
                local_user_lookup = json.load(f)
        except Exception as e:
            msg = (
                f"Unable to load local users ({e.__class__.__name__}) {e}\n"
                "  Will be unable to respond to any commands; update the lookup file"
                f" and either restart (stop, start) or SIGHUP ({os.getpid()}) this"
                " endpoint."
            )
            log.error(msg)

        valid_method_name_re = re.compile(r"^cmd_[A-Za-z][0-9A-Za-z_]{0,99}$")
        max_skew_s = 180  # 3 minutes; ignore commands with out-of-date timestamp
        while not self._time_to_stop:
            if self._wait_for_child:
                self.wait_for_children()

            try:
                _command = self._command_queue.get(timeout=1.0)
                d_tag, props, body = _command
                if props.headers and props.headers.get("debug", False):
                    body_log_b = _redact_url_creds(body, redact_user=False)
                    log.warning(
                        "Command debug requested:"
                        f"\n  Delivery Tag: {d_tag}"
                        f"\n  Properties: {props}"
                        f"\n  Body bytes: {body_log_b}"
                    )
            except queue.Empty:
                if self._command_stop_event.is_set():
                    self._time_to_stop = True
                if sys.stderr.isatty():
                    print(
                        f"\r{time.strftime('%c')}", end="", flush=True, file=sys.stderr
                    )
                continue

            try:
                server_cmd_ts = props.timestamp
                if props.content_type != "application/json":
                    raise ValueError("Invalid message type; expecting JSON")

                msg = json.loads(body)
                command = msg.get("command")
                command_args = msg.get("args", [])
                command_kwargs = msg.get("kwargs", {})
            except Exception as e:
                log.error(
                    f"Unable to deserialize Globus Compute services command."
                    f"  ({e.__class__.__name__}) {e}"
                )
                self._command.ack(d_tag)
                continue

            now = round(time.time())
            if abs(now - server_cmd_ts) > max_skew_s:
                server_pp_ts = datetime.fromtimestamp(server_cmd_ts).strftime("%c")
                endp_pp_ts = datetime.fromtimestamp(now).strftime("%c")
                log.warning(
                    "Ignoring command from server"
                    "\nCommand too old or skew between system clocks is too large."
                    f"\n  Command timestamp:  {server_cmd_ts:,} ({server_pp_ts})"
                    f"\n  Endpoint timestamp: {now:,} ({endp_pp_ts})"
                )
                self._command.ack(d_tag)
                continue

            try:
                globus_uuid = msg["globus_uuid"]
                globus_username = msg["globus_username"]
            except Exception as e:
                log.error(f"Invalid server command.  ({e.__class__.__name__}) {e}")
                self._command.ack(d_tag)
                continue

            try:
                local_user = local_user_lookup[globus_username]
            except Exception as e:
                log.warning(f"Invalid or unknown user.  ({e.__class__.__name__}) {e}")
                self._command.ack(d_tag)
                continue

            try:
                if not (command and valid_method_name_re.match(command)):
                    raise InvalidCommandError(f"Unknown or invalid command: {command}")

                command_func = getattr(self, command, None)
                if not command_func:
                    raise InvalidCommandError(f"Unknown or invalid command: {command}")

                command_func(local_user, command_args, command_kwargs)
                log.info(
                    f"Command process successfully forked for '{globus_username}'"
                    f" ('{globus_uuid}')."
                )
            except InvalidCommandError as err:
                log.error(str(err))

            except Exception:
                log.exception(
                    f"Unable to execute command: {command}\n"
                    f"    args: {command_args}\n"
                    f"  kwargs: {command_kwargs}"
                )
            finally:
                self._command.ack(d_tag)

    def cmd_start_endpoint(
        self,
        local_username: str,
        args: list[str] | None,
        kwargs: dict | None,
    ):
        if not args:
            args = []
        if not kwargs:
            kwargs = {}

        ep_name = kwargs.get("name", "")
        if not ep_name:
            raise InvalidCommandError("Missing endpoint name")

        proc_args = [
            "globus-compute-endpoint",
            "start",
            ep_name,
            "--die-with-parent",
            *args,
        ]

        pw_rec = pwd.getpwnam(local_username)
        udir, uid, gid = pw_rec.pw_dir, pw_rec.pw_uid, pw_rec.pw_gid
        uname = pw_rec.pw_name

        try:
            pid = os.fork()
        except Exception as e:
            log.error(f"Unable to fork child process: ({e.__class__.__name__}) {e}")
            raise

        if pid > 0:
            proc_args_s = f"({uname}, {ep_name}) {' '.join(proc_args)}"
            self._child_args[pid] = (uid, gid, local_username, proc_args_s)
            log.info(f"Creating new user endpoint (pid: {pid}) [{proc_args_s}]")
            return

        # Reminder: from this point on, we are now the *child* process.
        pid = os.getpid()

        import shutil  # in the child process; no need to load this in MTEP space

        exit_code = 70
        try:
            pybindir = pathlib.Path(sys.executable).parent
            default_path = ("/usr/local/bin", "/usr/bin", "/bin", pybindir)
            env: dict[str, str] = {"PATH": ":".join(map(str, default_path))}
            env_path = self.conf_dir / "user_environment.yaml"
            try:
                if env_path.exists():
                    log.debug("Load default environment variables from: %s", env_path)
                    env_text = env_path.read_text()
                    if env_text:
                        env.update(
                            {k: str(v) for k, v in yaml.safe_load(env_text).items()}
                        )

            except Exception as e:
                log.warning(
                    "Failed to parse user environment variables from %s.  Using "
                    "default: %s\n  --- Exception ---\n(%s) %s",
                    env_path,
                    env,
                    type(e).__name__,
                    e,
                )
            env.update({"HOME": udir, "USER": uname})
            if not os.path.isdir(udir):
                udir = "/"

            wd = env.get("PWD", udir)

            os.chdir("/")  # always succeeds, so start from known place
            exit_code += 1

            try:
                # The initialization of groups is "fungible" if not a privileged user
                log.debug("Initializing groups for %s, %s", uname, gid)
                os.initgroups(uname, gid)
            except PermissionError as e:
                log.warning("Unable to initialize groups; likely not a privileged user")
                log.debug("Exception text: (%s) %s", e.__class__.__name__, e)
            exit_code += 1

            # But actually becoming the correct UID is _not_ fungible.  If we
            # can't -- for whatever reason -- that's a problem.  So, don't ignore the
            # potential error.
            log.debug("Setting process group for %s to %s", pid, gid)
            os.setresgid(gid, gid, gid)  # raises (good!) on error
            exit_code += 1
            log.debug("Setting process uid for %s to %s (%s)", pid, uid, uname)
            os.setresuid(uid, uid, uid)  # raises (good!) on error
            exit_code += 1

            # some Q&D verification for admin debugging purposes
            if not shutil.which(proc_args[0], path=env["PATH"]):
                log.warning(
                    "Unable to find executable."
                    f"\n  Executable (not found): {proc_args[0]}"
                    f'\n  Path: "{env["PATH"]}"'
                    f"\n\n  Will attempt exec anyway -- WARNING - it will likely fail."
                    f"\n  (pid: {pid}, user: {uname}, {ep_name})"
                )

            os.setsid()
            exit_code += 1

            umask = 0o077  # Let child process set less restrictive, if desired
            log.debug("Setting process umask for %s to 0o%04o (%s)", pid, umask, uname)
            os.umask(umask)
            exit_code += 1

            log.debug("Changing directory to '%s'", wd)
            os.chdir(wd)
            exit_code += 1
            env["PWD"] = wd
            env["CWD"] = wd

            # in case "something gets stuck," let cmdline show it
            args_title = " ".join(proc_args)
            startup_proc_title = f"Endpoint starting up for {uname} [{args_title}]"
            setproctitle.setproctitle(startup_proc_title)

            gc_dir: pathlib.Path = GC.sdk.login_manager.tokenstore.ensure_compute_dir()
            (gc_dir / ep_name).mkdir(mode=0o700, parents=True, exist_ok=True)

            user_opts = kwargs.get("user_opts", {})
            stdin_data_dict = {
                "amqp_creds": kwargs.get("amqp_creds"),
                "config": render_config_user_template(self.conf_dir, user_opts),
            }
            stdin_data = json.dumps(stdin_data_dict)
            exit_code += 1

            # Reminder: this is *os*.open, not *open*.  Descriptors will not be closed
            # unless we explicitly do so, so `null_fd =` in loop will work.
            null_fd = os.open(os.devnull, os.O_WRONLY, mode=0o200)
            while null_fd < 3:  # reminder 0/1/2 == std in/out/err, so ...
                # ... overkill, but "just in case": don't step on them
                null_fd = os.open(os.devnull, os.O_WRONLY, mode=0o200)
            exit_code += 1

            log.debug("Setting up process stdin")
            read_handle, write_handle = os.pipe()

            # fcntl.F_GETPIPE_SZ is not available in Python versions less than 3.10
            F_GETPIPE_SZ = 1032
            # 256 - Allow some head room for multiple kernel-specific factors
            max_buf_size = fcntl.fcntl(write_handle, F_GETPIPE_SZ) - 256
            stdin_data_size = len(stdin_data)
            if stdin_data_size > max_buf_size:
                raise ValueError(
                    f"Unable to write {stdin_data_size} bytes of data to stdin; "
                    f"the maximum allowed is {max_buf_size} bytes"
                )

            exit_code += 1
            if os.dup2(read_handle, 0) != 0:  # close old stdin, use read_handle
                raise OSError("Unable to close stdin")
            os.close(read_handle)
            exit_code += 1

            log.debug("Redirecting stdout and stderr (%s)", os.devnull)
            with os.fdopen(null_fd, "w") as null_f:
                if os.dup2(null_f.fileno(), 1) != 1:
                    raise OSError("Unable to close stdout")
                exit_code += 1
                if os.dup2(null_f.fileno(), 2) != 2:
                    raise OSError("Unable to close stderr")

            # After the last os.dup2(), we are unable to get logs at *all*; hence the
            # exit_code as a last-ditch attempt at sharing "what went wrong where" to
            # the parent process.
            exit_code += 1
            log.debug("Writing credentials and config to stdin")
            with os.fdopen(write_handle, "w") as stdin_pipe:
                # intentional side effect: close handle
                stdin_pipe.write(stdin_data)

            exit_code += 1
            _soft_no, hard_no = resource.getrlimit(resource.RLIMIT_NOFILE)

            # Save closerange until last so that we can still get logs written
            # to the endpoint.log.  Meanwhile, use the exit_code as a
            # last-ditch attempt at sharing "what went wrong where" to the
            # parent process.
            exit_code += 1
            os.closerange(3, hard_no)

            exit_code += 1
            os.execvpe(proc_args[0], args=proc_args, env=env)

            # not executed, except perhaps in testing
            exit_code += 1  # type: ignore
        except Exception as e:
            log.error(f"Unable to exec for {uname} - ({e.__class__.__name__}) {e}")
        finally:
            # Only executed if execvpe fails (or isn't reached)
            exit(exit_code)
