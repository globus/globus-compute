from __future__ import annotations

import fcntl
import json
import logging
import os
import pathlib
import pwd
import resource
import sys

import setproctitle
from globus_compute_endpoint.endpoint.utils import send_endpoint_startup_failure_to_amqp

try:
    import pyprctl
except AttributeError as e:
    raise ImportError("pyprctl is not supported on this system") from e

import globus_compute_sdk as GC
import yaml
from globus_compute_endpoint.endpoint.config.utils import (
    load_user_config_template,
    render_config_user_template,
)

log = logging.getLogger(__name__)


def cmd_start_user_endpoint(
    uname: str,
    conf_dir: pathlib.Path,
    args: list[str],
    kwargs: dict,
):
    ep_name = kwargs.get("name", "")
    if not ep_name:
        raise OSError("Missing endpoint name")

    proc_args = [
        "globus-compute-endpoint",
        "start",
        ep_name,
        "--die-with-parent",
        *args,
    ]

    # in case "something gets stuck," let process entry show it
    args_title = " ".join(proc_args)
    startup_proc_title = f"Endpoint starting up for {uname} [{args_title}]"
    setproctitle.setproctitle(startup_proc_title)

    pid = os.getpid()
    mep_userrec = pwd.getpwuid(os.getuid())

    uep_userrec = pwd.getpwnam(uname)  # local username of unprivileged user
    udir, uid, gid = uep_userrec.pw_dir, uep_userrec.pw_uid, uep_userrec.pw_gid

    exit_code = 70
    try:
        import shutil
        from multiprocessing.process import current_process

        # hack to work with logging module; distinguish process states
        # beyond subtle pid: MainProcess-12345 --> UserEnd...(PreExec)-23456
        current_process().name = "UserEndpointProcess_Bootstrap(PreExec)"

        from globus_compute_endpoint.logging_config import setup_logging

        # after dropping privileges, any log.* calls may not be able to access
        # the parent's logging file.  We'll rely on stderr in that case, and fall
        # back to the exit_code in the worst case.
        setup_logging(logfile=None, debug=log.getEffectiveLevel() <= logging.DEBUG)

        # load prior to dropping privileges
        template_str, user_config_schema = load_user_config_template(conf_dir)

        pybindir = pathlib.Path(sys.executable).parent
        default_path = ("/usr/local/bin", "/usr/bin", "/bin", pybindir)
        env: dict[str, str] = {"PATH": ":".join(map(str, default_path))}
        env_path = conf_dir / "user_environment.yaml"
        try:
            if env_path.exists():
                log.debug("Load default environment variables from: %s", env_path)
                env_text = env_path.read_text()
                if env_text:
                    env_data = yaml.safe_load(env_text)
                    if env_data:
                        env.update({k: str(v) for k, v in env_data.items()})

        except Exception as e:
            log.warning(
                "Failed to parse user environment variables from %s.  Using "
                "default: %s\n  --- Exception ---\n(%s) %s",
                env_path,
                env,
                type(e).__name__,
                e,
            )
        user_home = {"HOME": udir, "USER": uname}
        env.update(user_home)
        os.environ.update(user_home)

        if not os.path.isdir(udir):
            udir = "/"

        wd = env.get("PWD", udir)

        os.chdir("/")  # always succeeds, so start from known place
        exit_code += 1

        if (os.getuid(), os.getgid()) != (uid, gid):
            # For multi-user systems, this is the expected path.  But for those
            # who run the multi-user setup as a non-privileged user, there is
            # no need to change the user: they're already executing _as that
            # uid_!
            try:
                # The initialization of groups is "fungible" if not a
                # privileged user
                log.debug("Initializing groups for %s, %s", uname, gid)
                os.initgroups(uname, gid)
            except PermissionError as e:
                log.warning(
                    "Unable to initialize groups; unprivileged user?  Ignoring"
                    " error, but further attempts to drop privileges may fail."
                    "\n  Process ID (pid): %s"
                    "\n  Current user: %s (uid: %s, gid: %s)"
                    "\n  Attempted to initgroups to: %s (uid: %s, name: %s)",
                    pid,
                    mep_userrec.pw_name,
                    os.getuid(),
                    os.getgid(),
                    gid,
                    uid,
                    uname,
                )
                log.debug("Exception text: (%s) %s", e.__class__.__name__, e)
            exit_code += 1

            # But actually becoming the correct UID is _not_ fungible.  If we
            # can't -- for whatever reason -- that's a problem.  So do NOT
            # ignore the potential error.
            log.debug("Setting process group for %s to %s", pid, gid)
            os.setresgid(gid, gid, gid)  # raises (good!) on error
            exit_code += 1
            log.debug("Setting process uid for %s to %s (%s)", pid, uid, uname)
            os.setresuid(uid, uid, uid)  # raises (good!) on error
            exit_code += 1

        # If we had any capabilities, we drop them now.
        pyprctl.CapState().set_current()

        # Even if exec'ing might return some privileges, "no."
        pyprctl.set_no_new_privs()

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

        os.environ["PWD"] = wd
        os.environ["CWD"] = wd
        env["PWD"] = wd
        env["CWD"] = wd

        gc_dir: pathlib.Path = GC.sdk.login_manager.tokenstore.ensure_compute_dir()
        (gc_dir / ep_name).mkdir(mode=0o700, parents=True, exist_ok=True)

        user_opts = kwargs.get("user_opts", {})
        user_config = render_config_user_template(
            template_str, user_config_schema, user_opts
        )
        stdin_data_dict = {
            "amqp_creds": kwargs.get("amqp_creds"),
            "config": user_config,
        }
        stdin_data = json.dumps(stdin_data_dict, separators=(",", ":"))
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
        # 256 - Allow some headroom for multiple kernel-specific factors
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
        msg = (
            f"Unable to start user endpoint process for {uname}"
            f" [exit code: {exit_code}; ({type(e).__name__}) {e}]"
        )
        try:
            log.error(msg)
            log.debug(f"Failed to exec for {uname}", exc_info=e)
            send_endpoint_startup_failure_to_amqp(kwargs["amqp_creds"], msg=msg)
        except Exception:
            # we're dying anyway;
            pass
    finally:
        # Only executed if execvpe fails (or isn't reached)
        sys.exit(exit_code)
