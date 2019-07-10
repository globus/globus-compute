import requests
import argparse
import logging
import os
import pathlib
import grp
import signal
import daemon
import lockfile
import uuid
import json
import sys
import platform
import getpass
import shutil

import funcx
from funcx.executors.high_throughput import sample_config

def reload_and_restart():
    print("Restarting funcX endpoint")

def foo():
    print("Start endpoint")
    import time
    for i in range(120):
        time.sleep(1)
        print("Running ep")

def init_endpoint(args):
    """ Setup funcx dirs and config files
    """

    if os.path.exists(args.config_file):
        logger.debug("Config file exists at {}".format(args.config_file))
        if not args.force:
           return

    funcx_dir = os.path.dirname(args.config_file)
    print("Funcx dir: ", funcx_dir)
    try:
        os.makedirs(funcx_dir, exist_ok=True)
    except Exception as e:
        print("[FuncX] Caught exception during registration {}".format(e))

    shutil.copyfile(sample_config.__file__, args.config_file)

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

def start_endpoint(args):
    """Start an endpoint

    This function will do:
    1. Connect to the broker service, and register itself
    2. Get connection info from broker service
    3. Start the interchange as a daemon

    +-------------+            +-------------+
    |             |            |             |
    |   /register |<-----------|     start   |
    |  Broker     |--reg_info->|   Endpoint  |
    |             |            |             |----> start interchange
    |             |            |             |
    +-------------+            +-------------+
    """

    print("Address: ", args.address)
    print("config_file: ", args.config_file)
    print("logdir: ", args.logdir)

    reg_info = register_with_hub(args.address)
    print("Reg_info: ", reg_info)

    optionals = {}
    optionals['logdir'] = args.logdir
    optionals['address'] = args.address

    if args.debug:
        optionals['logging_level'] = logging.DEBUG
    """
    with daemon.DaemonContext():
        ic = Interchange(**optionals)
    ic.start()
    """

    os.makedirs(args.logdir, exist_ok=True)

    try:
        context = daemon.DaemonContext(working_directory=args.logdir,
                                       umask=0o002,
                                       pidfile=lockfile.FileLock(
                                           os.path.join(args.logdir,
                                                        'funcx.{}.pid'.format(uuid.uuid4()))
                                       )

        )
    except Exception as e:
        print("Caught exception while trying to setup endpoint context dirs")
        print("Exception : ",e)

    context.signal_map = {signal.SIGTERM: stop_endpoint,
                          signal.SIGHUP: 'terminate',
                          signal.SIGUSR1: reload_and_restart}

    with context:
        foo()


def stop_endpoint():
    print("Terminating")


def register_endpoint(args):
    print("Register args : ", args)

def cli_run():

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')
    parser.add_argument("-v", "--version",
                        help="Print Endpoint version information")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enables debug logging")
    parser.add_argument("-c", "--config_file",
                        default='{}/.funcx/config.py'.format(pathlib.Path.home()),
                        help="Path to config file")

    setup = subparsers.add_parser('init',
                                  help='Sets up starter config files to help start running endpoints')
    setup.add_argument("-f", "--force", action='store_true',
                       help="Force re-initialization of config with this flag.\nWARNING: This will wipe your current config")

    register = subparsers.add_parser('register',
                                     help='Registers an endpoint with the web frontend')
    register.add_argument("-l", "--label", help="Label for the endpoint")

    start = subparsers.add_parser('start', help='Starts an endpoint')
    start.add_argument("-l", "--label", help="Label for the endpoint")

    stop = subparsers.add_parser('stop', help='Stops an active endpoint')

    _list = subparsers.add_parser('list', help='Lists all registered endpoints')

    args = parser.parse_args()

    funcx.set_stream_logger(level = logging.DEBUG if args.debug else logging.INFO)
    global logger
    logger = logging.getLogger('funcx')

    if args.version:
        logger.info("FuncX version: {}".format(__version__))

    logger.debug("Command: {}".format(args.command))

    if args.command == "init":
        init_endpoint(args)

    if not os.path.exists(args.config_file):
        logger.critical("Missing a config file at {}. Critical error. Exiting.".format(args.config_file))
        exit(-1)
    else:
        logger.debug("Using config file at {}".format(args.config_file))

    if args.command == "register":
        register_endpoint(args)
    elif args.command == "start":
        start_endpoint(args)
    elif args.command == "stop":
        stop_endpoint(args)
    elif args.command == "list":
        list_endpoints(args)

if __name__ == '__main__':

    cli_run()
