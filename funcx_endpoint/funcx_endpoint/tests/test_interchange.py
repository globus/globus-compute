import argparse
from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors.high_throughput.interchange import Interchange

import funcx

funcx.set_stream_logger()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--address", required=True,
                        help="Address")
    parser.add_argument("-c", "--client_ports", required=True,
                        help="ports")
    args = parser.parse_args()
    config = Config()

    ic = Interchange(config,
                     client_address=args.address,
                     client_ports=[int(i) for i in args.client_ports.split(',')],
                      )
    ic.start()
    print("Interchange started")
