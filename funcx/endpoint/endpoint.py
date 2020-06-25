import argparse
import glob
from importlib.machinery import SourceFileLoader
import json
import logging
import os
import pathlib
import random
import shutil
import signal
import sys
import time
import uuid

import daemon
import daemon.pidfile
import psutil
import requests
import texttable as tt
import typer

import funcx
from funcx.errors import *
from funcx.executors.high_throughput import default_config, global_config
from funcx.executors.high_throughput.interchange import Interchange
from funcx.sdk.client import FuncXClient

app = typer.Typer()


class State:
    DEBUG = False
    CONFIG_DIR = '{}/.funcx'.format(pathlib.Path.home())
    CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.py')
    GLOBAL_CONFIG_FILE = global_config.__file__
    GLOBAL_CONFIG = {}


def version_callback(value):
    if value:
        logger.info("Theodore's Version!")
        logger.info("FuncX version: {}".format(funcx.__version__))
        raise typer.Exit()


def complete_endpoint_name():
    config_files = glob.glob('{}/*/config.py'.format(State.CONFIG_DIR))
    return [
        os.path.basename(os.path.dirname(config_file))
        for config_file in config_files
    ]


def check_pidfile(filepath, match_name, endpoint_name):
    """ Helper function to identify possible dead endpoints
    """
    if not os.path.exists(filepath):
        return

    older_pid = int(open(filepath, 'r').read().strip())

    try:
        proc = psutil.Process(older_pid)
        if proc.name() == match_name:
            logger.info("Endpoint is already active")
    except psutil.NoSuchProcess:
        logger.info("A prior Endpoint instance appears to have been terminated without proper cleanup")
        logger.info('''Please cleanup using:
    $ funcx-endpoint stop {}'''.format(endpoint_name))


def init_endpoint_dir(endpoint_name, config=None):
    """ Initialize a clean endpoint dir

    Returns if an endpoint_dir already exists

    Parameters
    ----------

    funcx_dir : str
        Path to the funcx_dir on the system

    endpoint_name : str
        Name of the endpoint, which will be used to name the dir
        for the endpoint.

    config : str
       Path to a config file to be used instead of the funcX default config file

    """
    endpoint_dir = os.path.join(State.CONFIG_DIR, endpoint_name)
    os.makedirs(endpoint_dir, exist_ok=True)

    config = config if config else default_config.__file__
    shutil.copyfile(config,
                    os.path.join(endpoint_dir, 'config.py'))
    return endpoint_dir


@app.command(name="init")
def init_endpoint(
        force: bool = typer.Option(
            False, "--force", help="Force re-initialization of config with this flag. WARNING: This will wipe your "
                                   "current config "
        )
):
    """Setup funcx dirs and default endpoint config files

    TODO : Every mechanism that will update the config file, must be using a
    locking mechanism, ideally something like fcntl https://docs.python.org/3/library/fcntl.html
    to ensure that multiple endpoint invocations do not mangle the funcx config files
    or the lockfile module.
    """
    if force:
        _ = FuncXClient(force_login=True)
 
    if force and os.path.exists(State.CONFIG_DIR):
        logger.warning("Wiping all current configs in {}".format(State.CONFIG_DIR))
        try:
            logger.debug("Removing old backups in {}".format(State.CONFIG_DIR + '.bak'))
            shutil.rmtree(State.CONFIG_DIR + '.bak')
        except Exception:
            pass
        os.renames(State.CONFIG_DIR, State.CONFIG_DIR + '.bak')

    if os.path.exists(State.CONFIG_FILE):
        logger.debug("Config file exists at {}".format(State.CONFIG_FILE))
        return

    try:
        os.makedirs(State.CONFIG_DIR, exist_ok=True)
    except Exception as e:
        print("[FuncX] Caught exception during registration {}".format(e))

    shutil.copyfile(State.GLOBAL_CONFIG_FILE, State.CONFIG_FILE)
    init_endpoint_dir(State.CONFIG_DIR, "default")


def register_with_hub(endpoint_uuid, endpoint_dir, address,
                      redis_host='funcx-redis.wtgh6h.0001.use1.cache.amazonaws.com'):
    """ This currently registers directly with the Forwarder micro service.

    Can be used as an example of how to make calls this it, while the main API
    is updated to do this calling on behalf of the endpoint in the second iteration.
    """
    print("Picking source as a mock site")
    sites = ['128.135.112.73', '140.221.69.24',
             '52.86.208.63', '129.114.63.99',
             '128.219.134.72', '134.79.129.79']
    ip_addr = random.choice(sites)
    try:
        r = requests.post(address + '/register',
                          json={'endpoint_id': endpoint_uuid,
                                'endpoint_addr': ip_addr,
                                'redis_address': redis_host})
    except requests.exceptions.ConnectionError:
        logger.critical("Unable to reach the funcX hub at {}".format(address))
        exit(-1)
        # raise FuncXUnreachable(str(address))

    if r.status_code != 200:
        print(dir(r))
        print(r)
        raise RegistrationError(r.reason)

    with open(os.path.join(endpoint_dir, 'endpoint.json'), 'w+') as fp:
        json.dump(r.json(), fp)
        logger.debug("Registration info written to {}/endpoint.json".format(endpoint_dir))

    return r.json()


@app.command(name="configure", help="Configure an endpoint")
def configure_endpoint(
        name: str = typer.Argument("default", help="endpoint name", autocompletion=complete_endpoint_name),
        config: str = typer.Option(None, "--config", help="Config file to be used as template")
):
    """Configure an endpoint

    Drops a config.py template into the funcx configs directory.
    The template usually goes to ~/.funcx/<ENDPOINT_NAME>/config.py
    """
    endpoint_dir = os.path.join(State.CONFIG_DIR, name)
    new_config_file = os.path.join(endpoint_dir, 'config.py')

    if not os.path.exists(endpoint_dir):
        init_endpoint_dir(name, config=config)
        print('''A default profile has been create for <{}> at {}
Configure this file and try restarting with:
    $ funcx-endpoint start {}'''.format(name,
                                        new_config_file,
                                        name))
        return


@app.command(name="start")
def start_endpoint(
        name: str = typer.Argument("default", autocompletion=complete_endpoint_name),
        endpoint_uuid: str = typer.Option(None, help="The UUID for the endpoint to register with")
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
    |      Start      |---5---> Interchange
    |     Endpoint    |        daemon
    +-----------------+

    Parameters
    ----------
    args : args object
       Args object from the arg parsing

    global_config : dict
       Global config dict
    """

    funcx_client = FuncXClient()

    endpoint_dir = os.path.join(State.CONFIG_DIR, name)
    endpoint_json = os.path.join(endpoint_dir, 'endpoint.json')

    if not os.path.exists(endpoint_dir):
        print('''Endpoint {0} is not configured!
1. Please create a configuration template with:
   $ funcx-endpoint configure {0}
2. Update configuration
3. Start the endpoint.
        '''.format(name))
        return

    # If pervious registration info exists, use that
    if os.path.exists(endpoint_json):
        with open(endpoint_json, 'r') as fp:
            logger.debug("Connection info loaded from prior registration record")
            reg_info = json.load(fp)
            endpoint_uuid = reg_info['endpoint_id']
    elif not endpoint_uuid:
        endpoint_uuid = str(uuid.uuid4())

    print(f"Starting endpoint with uuid: {endpoint_uuid}")
    logger.debug(f"Starting endpoint with uuid: {endpoint_uuid}")

    # Create a daemon context
    stdout = open(os.path.join(endpoint_dir, './interchange.stdout'), 'w+')
    stderr = open(os.path.join(endpoint_dir, './interchange.stderr'), 'w+')
    try:
        context = daemon.DaemonContext(working_directory=endpoint_dir,
                                       umask=0o002,
                                       # lockfile.FileLock(
                                       pidfile=daemon.pidfile.PIDLockFile(os.path.join(endpoint_dir,
                                                                                       'daemon.pid')),
                                       stdout=stdout,
                                       stderr=stderr,
                                       )
    except Exception as e:
        logger.critical("Caught exception while trying to setup endpoint context dirs")
        logger.critical("Exception : ", e)

    check_pidfile(context.pidfile.path, "funcx-endpoint", name)

    # TODO : we need to load the config ? maybe not. This needs testing
    endpoint_config = SourceFileLoader(
        'config',
        os.path.join(endpoint_dir, 'config.py')).load_module()

    with context:
        while True:
            # Register the endpoint
            logger.debug("Registering endpoint")
            if State.GLOBAL_CONFIG.get('broker_test', False) is True:
                logger.warning("**************** BROKER State.DEBUG MODE *******************")
                reg_info = register_with_hub(endpoint_uuid,
                                             endpoint_dir,
                                             State.GLOBAL_CONFIG['broker_address'],
                                             State.GLOBAL_CONFIG['redis_host'])
            else:
                reg_info = register_endpoint(funcx_client, name, endpoint_uuid, endpoint_dir)

            logger.info("Endpoint registered with UUID: {}".format(reg_info['endpoint_id']))

            # Configure the parameters for the interchange
            optionals = {}
            optionals['client_address'] = reg_info['address']
            optionals['client_ports'] = reg_info['client_ports'].split(',')
            if 'endpoint_address' in State.GLOBAL_CONFIG:
                optionals['interchange_address'] = State.GLOBAL_CONFIG['endpoint_address']

            optionals['logdir'] = endpoint_dir
            # optionals['State.DEBUG'] = True

            if State.DEBUG:
                optionals['logging_level'] = logging.DEBUG

            ic = Interchange(endpoint_config.config, **optionals)
            ic.start()
            ic.stop()

            logger.critical("Interchange terminated.")
            time.sleep(10)

    stdout.close()
    stderr.close()

    logger.critical(f"Shutting down endpoint {endpoint_uuid}")


def register_endpoint(funcx_client, endpoint_name, endpoint_uuid, endpoint_dir):
    """Register the endpoint and return the registration info.

    Parameters
    ----------

    funcx_client : FuncXClient
        The auth'd client to communicate with the funcX service

    endpoint_name : str
        The name to register the endpoint with

    endpoint_uuid : str
        The uuid to register the endpoint with

    endpoint_dir : str
        The directory to write endpoint registration info into.

    """
    logger.debug("Attempting registration")
    logger.debug(f"Trying with eid : {endpoint_uuid}")
    reg_info = funcx_client.register_endpoint(endpoint_name, endpoint_uuid)

    with open(os.path.join(endpoint_dir, 'endpoint.json'), 'w+') as fp:
        json.dump(reg_info, fp)
        logger.debug("Registration info written to {}/endpoint.json".format(endpoint_dir))

    return reg_info


@app.command(name="stop")
def stop_endpoint(name: str = typer.Argument("default", autocompletion=complete_endpoint_name)):
    """ Stops an endpoint using the pidfile

    """

    endpoint_dir = os.path.join(State.CONFIG_DIR, name)
    pid_file = os.path.join(endpoint_dir, "daemon.pid")

    if os.path.exists(pid_file):
        logger.debug(f"{name} has a daemon.pid file")
        pid = None
        with open(pid_file, 'r') as f:
            pid = int(f.read())
        # Attempt terminating
        try:
            logger.debug("Signalling process: {}".format(pid))
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.1)
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.1)
            # Wait to confirm that the pid file disappears
            if not os.path.exists(pid_file):
                logger.info("Endpoint <{}> is now stopped".format(name))

        except OSError:
            logger.warning("Endpoint {} could not be terminated".format(name))
            logger.warning("Attempting Endpoint {} cleanup".format(name))
            os.remove(pid_file)
            sys.exit(-1)
    else:
        logger.info("Endpoint <{}> is not active.".format(name))


@app.command(name="list")
def list_endpoints():
    """ List all available endpoints
    """
    table = tt.Texttable()

    headings = ['Endpoint Name', 'Status', 'Endpoint ID']
    table.header(headings)

    config_files = glob.glob('{}/*/config.py'.format(State.CONFIG_DIR))
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


@app.callback()
def main(
        ctx: typer.Context,
        version: bool = typer.Option(None, "--version", "-v", callback=version_callback, is_eager=True),
        debug: bool = typer.Option(False, "--debug", "-d"),
        config_dir: str = typer.Option(State.CONFIG_DIR, "--config_dir", "-c", help="override default config dir")
):
    # Note: no docstring here; the docstring for @app.callback is used as a help message for overall app.
    # Sets up global variables in the State wrapper (debug flag, config dir, default config file).
    # For commands other than `init`, we ensure the existence of the config directory and file.

    global logger
    funcx.set_stream_logger(level=logging.DEBUG if debug else logging.INFO)
    logger = logging.getLogger('funcx')
    logger.debug("Command: {}".format(ctx.invoked_subcommand))

    # Set global state variables, to avoid passing them around as arguments all the time
    State.DEBUG = debug
    State.CONFIG_DIR = config_dir
    State.CONFIG_FILE = os.path.join(State.CONFIG_DIR, 'config.py')

    # If we are running the init command, then we don't need to check existence of config
    if ctx.invoked_subcommand == "init":
        return

    # Otherwise, we ensure that configs exist
    if not os.path.exists(State.CONFIG_FILE):
        logger.critical("Missing a config file at {}. Critical error. Exiting.".format(State.CONFIG_FILE))
        logger.info("Please run the following to create the appropriate config files : \n $> funcx-endpoint init")
        exit(-1)

    logger.debug("Loading config files from {}".format(State.CONFIG_DIR))

    global_config = SourceFileLoader('global_config', State.CONFIG_FILE).load_module()
    State.GLOBAL_CONFIG_FILE = global_config.__file__
    State.GLOBAL_CONFIG = global_config.global_options


def cli_run():
    """Entry point for setuptools to point to"""
    funcx.set_stream_logger(level=logging.INFO)
    global logger
    logger = logging.getLogger('funcx')

    app()


# if __name__ == '__main__':
#     app()
