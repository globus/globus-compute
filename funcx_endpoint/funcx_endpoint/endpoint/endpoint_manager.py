import glob
from importlib.machinery import SourceFileLoader
import json
import logging
import os
import pathlib
import shutil
import signal
import sys
import time
import uuid
from string import Template

import daemon
import daemon.pidfile
import psutil
import texttable as tt
import typer
from retry.api import retry_call

import funcx
import zmq

from funcx.utils.errors import *
from funcx_endpoint.endpoint import default_config as endpoint_default_config
from funcx_endpoint.executors.high_throughput import global_config as funcx_default_config
from funcx_endpoint.endpoint.interchange import EndpointInterchange
from funcx.sdk.client import FuncXClient


class EndpointManager:
    def __init__(self, logger):
        self.funcx_config_file_name = 'config.py'
        self.DEBUG = False
        self.funcx_dir = '{}/.funcx'.format(pathlib.Path.home())
        self.funcx_config_file = os.path.join(self.funcx_dir, self.funcx_config_file_name)
        self.funcx_default_config_template = funcx_default_config.__file__
        self.funcx_config = {}
        self.name = 'default'
        self.logger = logger

    def init_endpoint_dir(self, endpoint_config=None):
        """ Initialize a clean endpoint dir
        Returns if an endpoint_dir already exists

        Parameters
        ----------
        endpoint_config : str
            Path to a config file to be used instead of the funcX default config file
        """

        endpoint_dir = os.path.join(self.funcx_dir, self.name)
        self.logger.debug(f"Creating endpoint dir {endpoint_dir}")
        os.makedirs(endpoint_dir, exist_ok=True)

        endpoint_config_target_file = os.path.join(endpoint_dir, self.funcx_config_file_name)
        if endpoint_config:
            shutil.copyfile(endpoint_config, endpoint_config_target_file)
            return endpoint_dir

        endpoint_config = endpoint_default_config.__file__
        with open(endpoint_config) as r:
            endpoint_config_template = Template(r.read())

        endpoint_config_template = endpoint_config_template.substitute(name=self.name)
        with open(endpoint_config_target_file, "w") as w:
            w.write(endpoint_config_template)

        return endpoint_dir

    def configure_endpoint(self, name, endpoint_config):
        self.name = name
        endpoint_dir = os.path.join(self.funcx_dir, self.name)
        new_config_file = os.path.join(endpoint_dir, self.funcx_config_file_name)

        if not os.path.exists(endpoint_dir):
            self.init_endpoint_dir(endpoint_config=endpoint_config)
            print(f'A default profile has been create for <{self.name}> at {new_config_file}')
            print('Configure this file and try restarting with:')
            print(f'    $ funcx-endpoint start {self.name}')
        else:
            print(f'config dir <{self.name}> already exsits')
            raise Exception('ConfigExists')

    def init_endpoint(self):
        """Setup funcx dirs and default endpoint config files

        TODO : Every mechanism that will update the config file, must be using a
        locking mechanism, ideally something like fcntl https://docs.python.org/3/library/fcntl.html
        to ensure that multiple endpoint invocations do not mangle the funcx config files
        or the lockfile module.
        """
        _ = FuncXClient()

        if os.path.exists(self.funcx_config_file):
            typer.confirm(
                "Are you sure you want to initialize this directory? "
                f"This will erase everything in {self.funcx_dir}", abort=True
            )
            self.logger.info("Wiping all current configs in {}".format(self.funcx_dir))
            backup_dir = self.funcx_dir + ".bak"
            try:
                self.logger.debug(f"Removing old backups in {backup_dir}")
                shutil.rmtree(backup_dir)
            except OSError:
                pass
            os.renames(self.funcx_dir, backup_dir)

        if os.path.exists(self.funcx_config_file):
            self.logger.debug("Config file exists at {}".format(self.funcx_config_file))
            return

        try:
            os.makedirs(self.funcx_dir, exist_ok=True)
        except Exception as e:
            print("[FuncX] Caught exception during registration {}".format(e))

        shutil.copyfile(self.funcx_default_config_template, self.funcx_config_file)
        init_endpoint_dir(self.name)

    def check_endpoint_json(self, endpoint_json, endpoint_uuid):
        if os.path.exists(endpoint_json):
            with open(endpoint_json, 'r') as fp:
                self.logger.debug("Connection info loaded from prior registration record")
                reg_info = json.load(fp)
                endpoint_uuid = reg_info['endpoint_id']
        elif not endpoint_uuid:
            endpoint_uuid = str(uuid.uuid4())
        return endpoint_uuid

    def start_endpoint(self, name, endpoint_uuid):
        self.name = name

        endpoint_dir = os.path.join(self.funcx_dir, self.name)
        endpoint_json = os.path.join(endpoint_dir, 'endpoint.json')

        if not os.path.exists(endpoint_dir):
            print(f'Endpoint {self.name} is not configured!')
            print('1. Please create a configuration template with:')
            print(f'    funcx-endpoint configure {self.name}')
            print('2. Update configuration')
            print('3. Start the endpoint.')
            return

        # These certs need to be recreated for every registration
        keys_dir = os.path.join(endpoint_dir, 'certificates')
        os.makedirs(keys_dir, exist_ok=True)
        client_public_file, client_secret_file = zmq.auth.create_certificates(keys_dir, "endpoint")
        client_public_key, _ = zmq.auth.load_certificate(client_public_file)
        client_public_key = client_public_key.decode('utf-8')

        endpoint_config = SourceFileLoader('config',
                                           os.path.join(endpoint_dir, self.funcx_config_file_name)).load_module()

        # This is to ensure that at least 1 executor is defined
        if not endpoint_config.config.executors:
            raise Exception(f"Endpoint config file at {endpoint_dir} is missing executor definitions")

        funcx_client = FuncXClient(funcx_service_address=endpoint_config.config.funcx_service_address)

        endpoint_uuid = self.check_endpoint_json(endpoint_json, endpoint_uuid)

        self.logger.info(f"Starting endpoint with uuid: {endpoint_uuid}")

        # Create a daemon context
        # If we are running a full detached daemon then we will send the output to
        # log files, otherwise we can piggy back on our stdout
        if endpoint_config.config.detach_endpoint:
            stdout = open(os.path.join(endpoint_dir, endpoint_config.config.stdout), 'w+')
            stderr = open(os.path.join(endpoint_dir, endpoint_config.config.stderr), 'w+')
        else:
            stdout = sys.stdout
            stderr = sys.stderr

        try:
            context = daemon.DaemonContext(working_directory=endpoint_dir,
                                           umask=0o002,
                                           pidfile=daemon.pidfile.PIDLockFile(
                                               os.path.join(endpoint_dir, 'daemon.pid')),
                                           stdout=stdout,
                                           stderr=stderr,
                                           detach_process=endpoint_config.config.detach_endpoint)

        except Exception:
            self.logger.exception("Caught exception while trying to setup endpoint context dirs")

        self.check_pidfile(context.pidfile.path, "funcx-endpoint")

        with context:
            self.daemon_launch(funcx_client, endpoint_uuid, endpoint_dir, keys_dir)

    def daemon_launch(self, funcx_client, endpoint_uuid, endpoint_dir, keys_dir):
        # TODO : we need to load the config ? maybe not. This needs testing
        endpoint_config = SourceFileLoader(
                'config',
                os.path.join(endpoint_dir, self.funcx_config_file_name)).load_module()

        # Register the endpoint
        self.logger.info("Registering endpoint")
        reg_info = retry_call(self.register_endpoint, fargs=[funcx_client, endpoint_uuid, endpoint_dir], delay=10, max_delay=300, backoff=1.2)
        self.logger.info("Endpoint registered with UUID: {}".format(reg_info['endpoint_id']))

        # Configure the parameters for the interchange
        optionals = {}
        optionals['client_address'] = reg_info['public_ip']
        optionals['client_ports'] = reg_info['tasks_port'], reg_info['results_port'], reg_info['commands_port'],
        if 'endpoint_address' in self.funcx_config:
            optionals['interchange_address'] = self.funcx_config['endpoint_address']

            optionals['logdir'] = endpoint_dir

        if self.DEBUG:
            optionals['logging_level'] = logging.DEBUG

        ic = EndpointInterchange(endpoint_config.config,
                                 endpoint_id=endpoint_uuid,
                                 keys_dir=keys_dir,
                                 **optionals)
        ic.start()
        ic.stop()

        self.logger.critical("Interchange terminated.")

    # Avoid a race condition when starting the endpoint alongside the web service
    def register_endpoint(self, funcx_client, endpoint_uuid, endpoint_dir):
        """Register the endpoint and return the registration info.

        Parameters
        ----------

        funcx_client : FuncXClient
            The auth'd client to communicate with the funcX service

        endpoint_uuid : str
            The uuid to register the endpoint with

        endpoint_dir : str
            The directory to write endpoint registration info into.

        """
        self.logger.debug("Attempting registration")
        self.logger.debug(f"Trying with eid : {endpoint_uuid}")
        import funcx_endpoint
        reg_info = funcx_client.register_endpoint(self.name,
                                                  endpoint_uuid,
                                                  endpoint_version=funcx_endpoint.__version__)

        # the service will send back a message with a 'status'='error'
        # property if something went wrong
        if 'status' in reg_info and reg_info['status'] == 'error':
            msg = "Endpoint registration failed."
            if 'reason' in reg_info:
                msg = "Endpoint registration failed. Service fail reason provided: {}".format(reg_info['reason'])
            raise Exception(msg)

        # this is a backup error handler in case an endpoint ID is not sent back
        # from the service or a bad ID is sent back
        if 'endpoint_id' not in reg_info:
            raise Exception("Endpoint ID was not included in the service's registration response.")
        elif not isinstance(reg_info['endpoint_id'], str):
            raise Exception("Endpoint ID sent by the service was not a string.")

        with open(os.path.join(endpoint_dir, 'endpoint.json'), 'w+') as fp:
            json.dump(reg_info, fp)
            self.logger.debug("Registration info written to {}/endpoint.json".format(endpoint_dir))

        certs_dir = os.path.join(endpoint_dir, 'certificates')
        os.makedirs(certs_dir, exist_ok=True)
        server_keyfile = os.path.join(certs_dir, 'server.key')
        self.logger.debug(f"Writing server key to {server_keyfile}")
        try:
            with open(server_keyfile, 'w') as f:
                f.write(reg_info['forwarder_pubkey'])
                os.chmod(server_keyfile, 0o600)
        except Exception:
            self.logger.exception("Failed to write server certificate")

        return reg_info

    def stop_endpoint(self, name):
        self.name = name
        endpoint_dir = os.path.join(self.funcx_dir, self.name)
        pid_file = os.path.join(endpoint_dir, "daemon.pid")

        if os.path.exists(pid_file):
            self.logger.debug(f"{self.name} has a daemon.pid file")
            pid = None
            with open(pid_file, 'r') as f:
                pid = int(f.read())
            # Attempt terminating
            try:
                self.logger.debug("Signalling process: {}".format(pid))
                # For all the processes, including the deamon and its child process tree
                # Send SIGTERM to the processes
                # Wait for 200ms
                # Send SIGKILL to the processes that are still alive
                parent = psutil.Process(pid)
                processes = parent.children(recursive=True)
                processes.append(parent)
                for p in processes:
                    p.send_signal(signal.SIGTERM)
                terminated, alive = psutil.wait_procs(processes, timeout=0.2)
                for p in alive:
                    p.send_signal(signal.SIGKILL)
                # Wait to confirm that the pid file disappears
                if not os.path.exists(pid_file):
                    self.logger.info("Endpoint <{}> is now stopped".format(self.name))

            except OSError:
                self.logger.warning("Endpoint {} could not be terminated".format(self.name))
                self.logger.warning("Attempting Endpoint {} cleanup".format(self.name))
                os.remove(pid_file)
                sys.exit(-1)
        else:
            self.logger.info("Endpoint <{}> is not active.".format(self.name))

    def delete_endpoint(self, name):
        self.name = name
        endpoint_dir = os.path.join(self.funcx_dir, self.name)

        # If endpoint currently running, stop it.
        pid_file = os.path.join(endpoint_dir, "daemon.pid")
        active = os.path.exists(pid_file)
        if active:
            stop_endpoint(self.name)

        shutil.rmtree(endpoint_dir)

    def check_pidfile(self, filepath, match_name):
        """ Helper function to identify possible dead endpoints
        """
        if not os.path.exists(filepath):
            return

        older_pid = int(open(filepath, 'r').read().strip())

        try:
            proc = psutil.Process(older_pid)
            if proc.name() == match_name:
                self.logger.info("Endpoint is already active")
        except psutil.NoSuchProcess:
            self.logger.info("A prior Endpoint instance appears to have been terminated without proper cleanup")
            self.logger.info('''Please cleanup using:
        $ funcx-endpoint stop {}'''.format(self.name))

    def list_endpoints(self):
        table = tt.Texttable()

        headings = ['Endpoint Name', 'Status', 'Endpoint ID']
        table.header(headings)

        config_files = glob.glob('{}/*/config.py'.format(self.funcx_dir))
        for config_file in config_files:
            endpoint_dir = os.path.dirname(config_file)
            endpoint_name = os.path.basename(endpoint_dir)
            status = 'Initialized'
            endpoint_id = None

            endpoint_json = os.path.join(endpoint_dir, 'endpoint.json')
            if os.path.exists(endpoint_json):
                with open(endpoint_json, 'r') as f:
                    endpoint_info = json.load(f)
                    endpoint_id = endpoint_info['endpoint_id']
                if os.path.exists(os.path.join(endpoint_dir, 'daemon.pid')):
                    status = 'Active'
                else:
                    status = 'Inactive'

            table.add_row([endpoint_name, status, endpoint_id])

        s = table.draw()
        print(s)
