import requests
import argparse
import logging
import os
import pathlib
import grp
import signal
import time
import json
import daemon
import daemon.pidfile
import lockfile
import uuid
import json
import sys
import platform
import getpass
import shutil
import tarfile
import signal
import psutil
import random
import importlib.machinery

import funcx
from funcx.executors.high_throughput import global_config, default_config
from funcx.executors.high_throughput.interchange import Interchange
from funcx.endpoint.list_endpoints import list_endpoints
from funcx.sdk.client import FuncXClient
from funcx.errors import *


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

def load_endpoint(endpoint_dir):
    """
    Parameters
    ----------

    endpoint_dir : str
        endpoint directory path within funcx_dir
    """
    import importlib.machinery

    endpoint_config_file = endpoint_dir + 'config.py'
    endpoint_name = os.path.basename(endpoint_dir)

    if os.path.exists(endpoint_config_file):
        config = importlib.machinery.SourceFileLoader('{}_config'.format(endpoint_name),
                                                      endpoint_config_file).load_module()
    logger.debug("Loaded config for {}".format(endpoint_name))
    return config.config

def init_endpoint_dir(funcx_dir, endpoint_name, config_file=None):
    """ Initialize a clean endpoint dir

    Returns if an endpoint_dir already exists

    Parameters
    ----------

    funcx_dir : str
        Path to the funcx_dir on the system

    endpoint_name : str
        Name of the endpoint, which will be used to name the dir
        for the endpoint.

    config_file : str
       Path to a config file to be used instead of the funcX default config file

    """
    endpoint_dir = os.path.join(funcx_dir, endpoint_name)
    os.makedirs(endpoint_dir, exist_ok=True)

    if not config_file:
        config_file = default_config.__file__

    shutil.copyfile(config_file,
                    os.path.join(endpoint_dir, 'config.py'))
    return endpoint_dir


def init_endpoint(args):
    """ Setup funcx dirs and config files including a default endpoint config

    TODO : Every mechanism that will update the config file, must be using a
    locking mechanism, ideally something like fcntl https://docs.python.org/3/library/fcntl.html
    to ensure that multiple endpoint invocations do not mangle the funcx config files
    or the lockfile module.
    """
    funcx_dir = args.config_dir

    if args.force and os.path.exists(funcx_dir):
        logger.warning("Wiping all current configs in {}".format(funcx_dir))
        try:
            logger.debug("Removing old backups in {}".format(funcx_dir + '.bak'))
            shutil.rmtree(funcx_dir + '.bak')
        except Exception:
            pass
        os.renames(funcx_dir, funcx_dir + '.bak')

    if os.path.exists(args.config_file):
        logger.debug("Config file exists at {}".format(args.config_file))
        return

    try:
        os.makedirs(funcx_dir, exist_ok=True)
    except Exception as e:
        print("[FuncX] Caught exception during registration {}".format(e))

    shutil.copyfile(global_config.__file__, args.config_file)
    init_endpoint_dir(funcx_dir, "default")


def register_with_hub(address, redis_host='funcx-redis.wtgh6h.0001.use1.cache.amazonaws.com'):
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
                          json={'endpoint_id': str(uuid.uuid4()),
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

    return r.json()


def configure_endpoint(args, config_file=None, global_config=None):
    """Configure an endpoint

    Drops a config.py template into the funcx configs directory.
    The template usually goes to ~/.funcx/<ENDPOINT_NAME>/config.py

    Parameters
    ----------
    args : args object
       Args object from the arg parsing

    config_file : str
       Path to a config file to be used instead of the funcX default config file

    global_config : dict
       Global config dict
    """

    endpoint_dir = os.path.join(args.config_dir, args.name)
    endpoint_json = os.path.join(endpoint_dir, 'endpoint.json')

    if not os.path.exists(endpoint_dir):
        init_endpoint_dir(args.config_dir, args.name, config_file=config_file)
        print('''A default profile has been create for <{}> at {}
Configure this file and try restarting with:
    $ funcx-endpoint start {}'''.format(args.name,
                                        os.path.join(endpoint_dir, 'config.py'),
                                        args.name))
        return


def start_endpoint(args, global_config=None):
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

    endpoint_dir = os.path.join(args.config_dir, args.name)
    endpoint_json = os.path.join(endpoint_dir, 'endpoint.json')

    if not os.path.exists(endpoint_dir):
        print('''Endpoint {0} is not configured!
1. Please create a configuration template with:
   $ funcx-endpoint configure {0}
2. Update configuration
3. Start the endpoint.
        '''.format(args.name))
        return

    # If pervious registration info exists, use that
    if os.path.exists(endpoint_json):
        with open(endpoint_json, 'r') as fp:
            logger.debug("Connection info loaded from prior registration record")
            reg_info = json.load(fp)
            endpoint_uuid = reg_info['endpoint_id']
    elif args.endpoint_uuid:
        endpoint_uuid = args.endpoint_uuid
    else:
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

    check_pidfile(context.pidfile.path, "funcx-endpoint", args.name)

    # TODO : we need to load the config ? maybe not. This needs testing
    endpoint_config = importlib.machinery.SourceFileLoader(
        'config',
        os.path.join(endpoint_dir, 'config.py')).load_module()

    with context:
        while True:
            # Register the endpoint
            logger.debug("Registering endpoint")
            if global_config.get('broker_test', False) is True:
                logger.warning("**************** BROKER DEBUG MODE *******************")
                reg_info = register_with_hub(global_config['broker_address'],
                                             global_config['redis_host'])
            else:
                reg_info = register_endpoint(funcx_client, args.name, endpoint_uuid, endpoint_dir)

            logger.info("Endpoint registered with UUID: {}".format(reg_info['endpoint_id']))

            # Configure the parameters for the interchange
            optionals = {}
            optionals['client_address'] = reg_info['address']
            optionals['client_ports'] = reg_info['client_ports'].split(',')
            if 'endpoint_address' in global_config:
                optionals['interchange_address'] = global_config['endpoint_address']

            optionals['logdir'] = endpoint_dir
            # optionals['debug'] = True

            if args.debug:
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


def stop_endpoint(args, global_config=None):
    """ Stops an endpoint using the pidfile

    Parameters
    ----------

    args
    global_config

    """

    endpoint_dir = os.path.join(args.config_dir, args.name)
    pid_file = os.path.join(endpoint_dir, "daemon.pid")

    if os.path.exists(pid_file):
        logger.debug("{} has a daemon.pid file".format(args.name))
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
                logger.info("Endpoint <{}> is now stopped".format(args.name))

        except OSError:
            logger.warning("Endpoint {} could not be terminated".format(args.name))
            logger.warning("Attempting Endpoint {} cleanup".format(args.name))
            os.remove(pid_file)
            sys.exit(-1)
    else:
        logger.info("Endpoint <{}> is not active.".format(args.name))


def cli_run():
    """ Entry point for funcx-endpoint
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')
    parser.add_argument("-v", "--version",
                        help="Print Endpoint version information")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enables debug logging")
    parser.add_argument("-c", "--config_dir",
                        default='{}/.funcx'.format(pathlib.Path.home()),
                        help="Path to funcx config directory")

    # Init Endpoint
    init = subparsers.add_parser('init',
                                 help='Sets up starter config files to help start running endpoints')
    init.add_argument("-f", "--force", action='store_true',
                      help="Force re-initialization of config with this flag.\nWARNING: This will wipe your current config")

    # Configure an endpoint
    configure = subparsers.add_parser('configure',
                                  help='Configures an endpoint')
    configure.add_argument("--config", default=None,
                           help="Path to config file to be used as template rather than the funcX default")
    configure.add_argument("name", help="Name of the endpoint to configure for")

    # Start an endpoint
    start = subparsers.add_parser('start',
                                  help='Starts an endpoint')
    start.add_argument("name", help="Name of the endpoint to start")
    start.add_argument("--endpoint_uuid", help="The UUID for the endpoint to register with",
                       default=None, required=False)

    # Stop an endpoint
    stop = subparsers.add_parser('stop', help='Stops an active endpoint')
    stop.add_argument("name", help="Name of the endpoint to stop")

    # List all endpoints
    subparsers.add_parser('list', help='Lists all endpoints')

    args = parser.parse_args()

    funcx.set_stream_logger(level=logging.DEBUG if args.debug else logging.INFO)
    global logger
    logger = logging.getLogger('funcx')

    if args.version:
        logger.info("FuncX version: {}".format(funcx.__version__))

    logger.debug("Command: {}".format(args.command))

    args.config_file = os.path.join(args.config_dir, 'config.py')

    if args.command == "init":
        if args.force:
            logger.debug("Forcing re-authentication via GlobusAuth")
        funcx_client = FuncXClient(force_login=args.force)
        init_endpoint(args)
        return

    if not os.path.exists(args.config_file):
        logger.critical("Missing a config file at {}. Critical error. Exiting.".format(args.config_file))
        logger.info("Please run the following to create the appropriate config files : \n $> funcx-endpoint init")
        exit(-1)

    if args.command == "init":
        init_endpoint(args)
        exit(-1)

    logger.debug("Loading config files from {}".format(args.config_dir))

    import importlib.machinery
    global_config = importlib.machinery.SourceFileLoader('global_config',
                                                         args.config_file).load_module()

    if args.command == "configure":
        configure_endpoint(args, config_file=args.config,
                           global_config=global_config.global_options)

    elif args.command == "start":
        start_endpoint(args, global_config=global_config.global_options)

    elif args.command == "stop":
        stop_endpoint(args, global_config=global_config.global_options)

    elif args.command == "list":
        list_endpoints(args)


if __name__ == '__main__':
    cli_run()
