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

import funcx
from funcx.executors.high_throughput import global_config, default_config
from funcx.executors.high_throughput.interchange import Interchange
from funcx.endpoint.list_endpoints import list_endpoints


def reload_and_restart():
    print("Restarting funcX endpoint")


def foo():
    print("Start endpoint")
    import time
    for i in range(120):
        time.sleep(1)
        print("Running ep")


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


def init_endpoint_dir(funcx_dir, endpoint_name):
    """ Initialize a clean endpoint dir

    Returns if an endpoint_dir already exists

    Parameters
    ----------

    funcx_dir : str
        Path to the funcx_dir on the system

    endpoint_name : str
        Name of the endpoint, which will be used to name the dir
        for the endpoint.
    """
    endpoint_dir = os.path.join(funcx_dir, endpoint_name)
    os.makedirs(endpoint_dir, exist_ok=True)
    shutil.copyfile(default_config.__file__,
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


def register_with_hub(address):
    r = requests.post(address + '/register',
                      json={'python_v': "{}.{}".format(sys.version_info.major,
                                                       sys.version_info.minor),
                            'os': platform.system(),
                            'hname': platform.node(),
                            'username': getpass.getuser(),
                            'funcx_v': str(funcx.__version__)
                      }
    )
    if r.status_code != 200:
        print("Caught an issue with the registration: ", r)

    return r.json()


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

    endpoint_dir = os.path.join(args.config_dir, args.name)
    endpoint_json = os.path.join(endpoint_dir, 'endpoint.json')

    if not os.path.exists(endpoint_dir):
        init_endpoint_dir(args.config_dir, args.name)
        print('''A default profile has been create for <{}> at {}
Configure this file and try restarting with:
    $ funcx-endpoint start {}'''.format(args.name,
                                        os.path.join(endpoint_dir, 'config.py'),
                                        args.name))
        return

    if os.path.exists(endpoint_json):
        with open(endpoint_json, 'r') as fp:
            logger.debug("Connection info loaded from prior registration record")
            reg_info = json.load(fp)
    else:
        logger.debug("Endpoint prior connection record not available. Attempting registration")
        reg_info = register_with_hub('http://{}:{}'.format(global_config['broker_address'],
                                                           global_config['broker_port']))

        logger.info("Registration info from broker: {}".format(reg_info))
        with open(os.path.join(endpoint_dir, 'endpoint.json'), 'w+') as fp:
            json.dump(reg_info, fp)
            logger.debug("Registration info written to {}/endpoint.json".format(endpoint_dir))

    optionals = {}
    optionals['client_address'] = reg_info['address']
    optionals['client_ports'] = reg_info['client_ports'].split(',')

    optionals['logdir'] = endpoint_dir
    # optionals['debug'] = True

    if args.debug:
        optionals['logging_level'] = logging.DEBUG

    import importlib.machinery
    endpoint_config = importlib.machinery.SourceFileLoader(
        'config',
        os.path.join(endpoint_dir,'config.py')).load_module()
    # TODO : we need to load the config ? maybe not. This needs testing

    stdout = open('./interchange.stdout', 'w+')
    stderr = open('./interchange.stderr', 'w+')
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
        print("Caught exception while trying to setup endpoint context dirs")
        print("Exception : ", e)

    with context:
        ic = Interchange(endpoint_config.config, **optionals)
        ic.start()

    stdout.close()
    stderr.close()

    print("Done")


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
        with open(pid_file, 'r') as f:
            pid = int(f.read())
            # Attempt terminating
            try:
                logger.debug("Signalling process: {}".format(pid))
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
                # Wait to confirm that the pid file disappears
                if not os.path.exists(pid_file):
                    logger.info("Endpoint <{}> is now stopped".format(args.name))

            except OSError:
                logger.warning("Endpoint {} could not be terminated".format(args.name))
                sys.exit(-1)
    else:
        logger.info("Endpoint <{}> is not active.".format(args.name))


def register_endpoint(args):
    print("Register args : ", args)


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

    # Start an endpoint
    start = subparsers.add_parser('start',
                                  help='Starts an endpoint')
    start.add_argument("name", help="Name of the endpoint to start")

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
        init_endpoint(args)

    if not os.path.exists(args.config_file):
        logger.critical("Missing a config file at {}. Critical error. Exiting.".format(args.config_file))
        logger.info("Please run the following to create the appropriate config files : \n $> funcx-endpoint init")
        exit(-1)
    else:
        logger.debug("Using config files from {}".format(args.config_dir))

    import importlib.machinery
    global_config = importlib.machinery.SourceFileLoader('global_config',
                                                         args.config_file).load_module()

    if args.command == "start":
        start_endpoint(args, global_config=global_config.global_options)
    elif args.command == "stop":
        stop_endpoint(args, global_config=global_config.global_options)
    elif args.command == "list":
        list_endpoints(args)


if __name__ == '__main__':
    cli_run()
