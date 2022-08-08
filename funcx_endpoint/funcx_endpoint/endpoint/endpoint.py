import glob
import json
import logging
import os
import pathlib
import shutil
import signal
import sys
import uuid
from string import Template

import click
import daemon
import daemon.pidfile
import psutil
import texttable
from globus_sdk import GlobusAPIError, NetworkError

from funcx.errors import FuncxResponseError
from funcx.sdk.client import FuncXClient
from funcx_endpoint.endpoint import default_config as endpoint_default_config
from funcx_endpoint.endpoint.interchange import EndpointInterchange
from funcx_endpoint.endpoint.register_endpoint import register_endpoint
from funcx_endpoint.endpoint.result_store import ResultStore
from funcx_endpoint.logging_config import setup_logging

log = logging.getLogger(__name__)


_DEFAULT_FUNCX_DIR = str(pathlib.Path.home() / ".funcx")


class Endpoint:
    """
    Endpoint is primarily responsible for configuring, launching and stopping
    the Endpoint.
    """

    def __init__(self, funcx_dir=None, debug=False):
        """Initialize the Endpoint

        Parameters
        ----------

        funcx_dir: str
            Directory path to the root of the funcx dirs. Usually ~/.funcx.

        debug: Bool
            Enable debug logging. Default: False
        """
        self.debug = debug
        self.funcx_dir = funcx_dir if funcx_dir is not None else _DEFAULT_FUNCX_DIR
        self.name = "default"

    @property
    def _endpoint_dir(self):
        return os.path.join(self.funcx_dir, self.name)

    @property
    def _config_file_path(self):
        return os.path.join(self._endpoint_dir, "config.py")

    def init_endpoint_dir(self, endpoint_config=None):
        """Initialize a clean endpoint dir
        Returns if an endpoint_dir already exists

        Parameters
        ----------
        endpoint_config : str
            Path to a config file to be used instead of the funcX default config file
        """

        log.debug(f"Creating endpoint dir {self._endpoint_dir}")
        os.makedirs(self._endpoint_dir, exist_ok=True)

        endpoint_config_target_file = self._config_file_path
        if endpoint_config:
            shutil.copyfile(endpoint_config, endpoint_config_target_file)
            return self._endpoint_dir

        endpoint_config = endpoint_default_config.__file__
        with open(endpoint_config) as r:
            endpoint_config_template = Template(r.read())

        endpoint_config_template = endpoint_config_template.substitute(name=self.name)
        with open(endpoint_config_target_file, "w") as w:
            w.write(endpoint_config_template)

        return self._endpoint_dir

    def configure_endpoint(self, name, endpoint_config):
        self.name = name

        if not os.path.exists(self._endpoint_dir):
            self.init_endpoint_dir(endpoint_config=endpoint_config)
            print(
                f"A default profile has been create for <{self.name}> "
                f"at {self._config_file_path}"
            )
            print("Configure this file and try restarting with:")
            print(f"    $ funcx-endpoint start {self.name}")
        else:
            print(f"config dir <{self.name}> already exsits")
            raise Exception("ConfigExists")

    def init_endpoint(self):
        """Setup funcx dirs and default endpoint config files

        TO-DO : Every mechanism that will update the config file, must be using a
        locking mechanism, ideally something like fcntl [1]
        to ensure that multiple endpoint invocations do not mangle the funcx config
        files or the lockfile module.

        [1] https://docs.python.org/3/library/fcntl.html
        """
        # construct client to force login flow
        # FIXME: `funcx` should provide a cleaner way of doing login
        FuncXClient(do_version_check=False)

        if os.path.exists(self.funcx_dir):
            click.confirm(
                "Are you sure you want to initialize this directory? "
                f"This will erase everything in {self.funcx_dir}",
                abort=True,
            )
            log.info(f"Wiping all current configs in {self.funcx_dir}")
            backup_dir = self.funcx_dir + ".bak"
            try:
                log.debug(f"Removing old backups in {backup_dir}")
                shutil.rmtree(backup_dir)
            except OSError:
                pass
            os.renames(self.funcx_dir, backup_dir)

        try:
            os.makedirs(self.funcx_dir, exist_ok=True)
        except Exception as e:
            print(f"[FuncX] Caught exception during registration {e}")

    def check_endpoint_json(self, endpoint_json, endpoint_uuid):
        if os.path.exists(endpoint_json):
            with open(endpoint_json) as fp:
                log.debug("Connection info loaded from prior registration record")
                reg_info = json.load(fp)
                endpoint_uuid = reg_info["endpoint_id"]
        elif not endpoint_uuid:
            endpoint_uuid = str(uuid.uuid4())
        return endpoint_uuid

    def start_endpoint(self, name, endpoint_uuid, endpoint_config):
        self.name = name

        endpoint_dir = os.path.join(self.funcx_dir, self.name)
        endpoint_json = os.path.join(endpoint_dir, "endpoint.json")

        # This is to ensure that at least 1 executor is defined
        if not endpoint_config.config.executors:
            raise Exception(
                f"Endpoint config file at {endpoint_dir} is missing "
                "executor definitions"
            )

        funcx_client_options = {
            "funcx_service_address": endpoint_config.config.funcx_service_address,
            "results_ws_uri": endpoint_config.config.results_ws_uri,
            "environment": endpoint_config.config.environment,
            "warn_about_url_mismatch": endpoint_config.config.warn_about_url_mismatch,
        }
        funcx_client = FuncXClient(**funcx_client_options)

        endpoint_uuid = self.check_endpoint_json(endpoint_json, endpoint_uuid)

        log.info(f"Starting endpoint with uuid: {endpoint_uuid}")

        pid_file = os.path.join(endpoint_dir, "daemon.pid")
        pid_check = self.check_pidfile(pid_file)
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
                self.pidfile_cleanup(pid_file)

        result_store = ResultStore(endpoint_dir=endpoint_dir)

        # Create a daemon context
        # If we are running a full detached daemon then we will send the output to
        # log files, otherwise we can piggy back on our stdout
        if endpoint_config.config.detach_endpoint:
            stdout = open(
                os.path.join(endpoint_dir, endpoint_config.config.stdout), "a+"
            )
            stderr = open(
                os.path.join(endpoint_dir, endpoint_config.config.stderr), "a+"
            )
        else:
            stdout = sys.stdout
            stderr = sys.stderr

        try:
            context = daemon.DaemonContext(
                working_directory=endpoint_dir,
                umask=0o002,
                pidfile=daemon.pidfile.PIDLockFile(pid_file),
                stdout=stdout,
                stderr=stderr,
                detach_process=endpoint_config.config.detach_endpoint,
            )

        except Exception:
            log.exception(
                "Caught exception while trying to setup endpoint context dirs"
            )
            sys.exit(-1)

        # place registration after everything else so that the endpoint will
        # only be registered if everything else has been set up successfully
        reg_info = None
        try:
            reg_info = register_endpoint(
                funcx_client, endpoint_uuid, endpoint_dir, self.name
            )
        # if the service sends back an error response, it will be a FuncxResponseError
        except FuncxResponseError as e:
            # an example of an error that could conceivably occur here would be
            # if the service could not register this endpoint with the forwarder
            # because the forwarder was unreachable
            if e.http_status_code >= 500:
                log.exception("Caught exception while attempting endpoint registration")
                log.critical(
                    "Endpoint registration will be retried in the new endpoint daemon "
                    "process. The endpoint will not work until it is successfully "
                    "registered."
                )
            else:
                raise e
        # if the service has an unexpected internal error and is unable to send
        # back a FuncxResponseError
        except GlobusAPIError as e:
            if e.http_status >= 500:
                log.exception("Caught exception while attempting endpoint registration")
                log.critical(
                    "Endpoint registration will be retried in the new endpoint daemon "
                    "process. The endpoint will not work until it is successfully "
                    "registered."
                )
            else:
                raise e
        # if the service is unreachable due to a timeout or connection error
        except NetworkError as e:
            # the output of a NetworkError exception is huge and unhelpful, so
            # it seems better to just stringify it here and get a concise error
            log.exception(
                f"Caught exception while attempting endpoint registration: {e}"
            )
            log.critical(
                "funcx-endpoint is unable to reach the funcX service due to a "
                "NetworkError \n"
                "Please make sure that the funcX service address you provided is "
                "reachable \n"
                "and then attempt restarting the endpoint"
            )
            exit(-1)
        except Exception:
            raise

        if reg_info:
            log.info("Launching endpoint daemon process")
        else:
            log.critical("Launching endpoint daemon process with errors noted above")

        # NOTE
        # It's important that this log is emitted before we enter the daemon context
        # because daemonization closes down everything, a log message inside the
        # context won't write the currently configured loggers
        logfile = os.path.join(endpoint_dir, "endpoint.log")
        log.info(
            "Logging will be reconfigured for the daemon. logfile=%s , debug=%s",
            logfile,
            self.debug,
        )

        with context:
            setup_logging(logfile=logfile, debug=self.debug, console_enabled=False)
            self.daemon_launch(
                endpoint_uuid,
                endpoint_dir,
                endpoint_config,
                reg_info,
                funcx_client,
                result_store,
            )

    def daemon_launch(
        self,
        endpoint_uuid,
        endpoint_dir,
        endpoint_config,
        reg_info,
        funcx_client: FuncXClient,
        result_store: ResultStore,
    ):
        interchange = EndpointInterchange(
            config=endpoint_config.config,
            reg_info=reg_info,
            endpoint_id=endpoint_uuid,
            endpoint_dir=endpoint_dir,
            endpoint_name=self.name,
            funcx_client=funcx_client,
            result_store=result_store,
            logdir=endpoint_dir,
        )

        interchange.start()

        log.critical("Interchange terminated.")

    def stop_endpoint(self, name):
        self.name = name
        endpoint_dir = os.path.join(self.funcx_dir, self.name)
        pid_file = os.path.join(endpoint_dir, "daemon.pid")
        pid_check = self.check_pidfile(pid_file)

        # The process is active if the PID file exists and the process it points to is
        # a funcx-endpoint
        if pid_check["active"]:
            log.debug(f"{self.name} has a daemon.pid file")
            pid = None
            with open(pid_file) as f:
                pid = int(f.read())
            # Attempt terminating
            try:
                log.debug(f"Signalling process: {pid}")
                # For all the processes, including the deamon and its child process tree
                # Send SIGTERM to the processes
                # Wait for 200ms
                # Send SIGKILL to the processes that are still alive
                parent = psutil.Process(pid)
                processes = parent.children(recursive=True)
                processes.append(parent)
                for p in processes:
                    p.send_signal(signal.SIGTERM)
                # graceful RabbitMQ cleanup takes some time
                terminated, alive = psutil.wait_procs(processes, timeout=10)
                for p in alive:
                    # sometimes a process that was marked as alive before can terminate
                    # before this signal is sent
                    try:
                        p.send_signal(signal.SIGKILL)
                    except psutil.NoSuchProcess:
                        pass
                # Wait to confirm that the pid file disappears
                if not os.path.exists(pid_file):
                    log.info(f"Endpoint <{self.name}> is now stopped")

            except OSError:
                log.warning(f"Endpoint <{self.name}> could not be terminated")
                log.warning(f"Attempting Endpoint <{self.name}> cleanup")
                os.remove(pid_file)
                sys.exit(-1)
        # The process is not active, but the PID file exists and needs to be deleted
        elif pid_check["exists"]:
            self.pidfile_cleanup(pid_file)
        else:
            log.info(f"Endpoint <{self.name}> is not active.")

    def delete_endpoint(self, name):
        self.name = name
        endpoint_dir = os.path.join(self.funcx_dir, self.name)

        if not os.path.exists(endpoint_dir):
            log.warning(f"Endpoint <{self.name}> does not exist")
            sys.exit(-1)

        # stopping the endpoint should handle all of the process cleanup before
        # deletion of the directory
        self.stop_endpoint(self.name)

        shutil.rmtree(endpoint_dir)
        log.info(f"Endpoint <{self.name}> has been deleted.")

    def check_pidfile(self, filepath):
        """Helper function to identify possible dead endpoints

        Returns a record with 'exists' and 'active' fields indicating
        whether the pidfile exists, and whether the process is active if it does exist
        (The endpoint can only start correctly if there is no pidfile)

        Parameters
        ----------
        filepath : str
            Path to the pidfile
        """
        if not os.path.exists(filepath):
            return {"exists": False, "active": False}

        pid = int(open(filepath).read().strip())

        active = False
        try:
            psutil.Process(pid)
        except psutil.NoSuchProcess:
            pass
        else:
            # this is the only case where the endpoint is active. If no process exists,
            # it means the endpoint has been terminated without proper cleanup
            active = True

        return {"exists": True, "active": active}

    def pidfile_cleanup(self, filepath):
        os.remove(filepath)
        log.info(f"Endpoint <{self.name}> has been cleaned up.")

    def list_endpoints(self):
        table = texttable.Texttable()

        headings = ["Endpoint Name", "Status", "Endpoint ID"]
        table.header(headings)

        config_files = glob.glob(os.path.join(self.funcx_dir, "*", "config.py"))
        for config_file in config_files:
            endpoint_dir = os.path.dirname(config_file)
            endpoint_name = os.path.basename(endpoint_dir)
            status = "Initialized"
            endpoint_id = None

            endpoint_json = os.path.join(endpoint_dir, "endpoint.json")
            if os.path.exists(endpoint_json):
                with open(endpoint_json) as f:
                    endpoint_info = json.load(f)
                    endpoint_id = endpoint_info["endpoint_id"]
                pid_check = self.check_pidfile(os.path.join(endpoint_dir, "daemon.pid"))
                if pid_check["active"]:
                    status = "Running"
                elif pid_check["exists"]:
                    status = "Disconnected"
                else:
                    status = "Stopped"

            table.add_row([endpoint_name, status, endpoint_id])

        s = table.draw()
        print(s)
