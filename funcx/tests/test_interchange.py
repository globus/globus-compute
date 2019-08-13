import argparse
from funcx.config import Config
from funcx.executors.high_throughput.interchange import Interchange
config = Config()

import funcx

funcx.set_stream_logger()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--address",
                        help="Address")
    parser.add_argument("-c", "--client_ports",
                        help="ports")
    args = parser.parse_args()
    
    ic =  Interchange(config,
                      client_address=args.address,
                      client_ports=[int(i) for i in args.client_ports.split(',')],
                      )
    ic.start()

    

