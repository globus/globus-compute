import requests
import argparse
import logging
import os
import grp
import signal
import daemon
import lockfile
import uuid
from funcx import __version__

def reload_and_restart():
    print("Restarting funcX endpoint")

def foo():
    print("Start endpoint")
    import time
    for i in range(120):
        time.sleep(1)
        print("Running ep")

def start_endpoint(args):
    """Start an endpoint

    This function will do:
    1. Connect to the broker service, and register itself
    2. Get connection info from broker service
    3. Start the interchange as a daemon
    """

    print("Address: ", args.address)
    print("config_file: ", args.config_file)
    print("logdir: ", args.logdir)

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


def cli_run():

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')
    parser.add_argument("-v", "--version",
                        help="Print Endpoint version information")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enables debug logging")

    start = subparsers.add_parser('start', help='Starts an endpoint') #, dest='command')
    start.add_argument("-a", "--address", default="127.0.0.1:8888",
                        help="Address of the hub to which the endpoint should connect")
    start.add_argument("-l", "--logdir", default="endpoint_logs",
                        help="Path to endpoint log directory")
    start.add_argument("-c", "--config_file", default=None,
                        help="Path to config file")

    stop = subparsers.add_parser('stop', help='Stops an endpoint') # , dest='command')

    _list = subparsers.add_parser('list', help='Lists all active endpoints') #, dest='command')

    args = parser.parse_args()

    if args.version:
        print("FuncX version: {}".format(__version__))

    print("Command: ", args.command)

    if args.command == "start":
        start_endpoint(args)
    elif args.command == "stop":
        stop_endpoint(args)
    elif args.command == "list":
        list_endpoints(args)

if __name__ == '__main__':
    cli_run()
