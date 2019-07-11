import argparse
import requests
import funcx
import sys
import platform
import getpass


def test(address):
    r = requests.post(address + '/register',
                      json={'python_v': "{}.{}".format(sys.version_info.major,
                                                       sys.version_info.minor),
                            'os': platform.system(),
                            'hname': platform.node(),
                            'username': getpass.getuser(),
                            'funcx_v': str(funcx.__version__)
                      }
    )
    print("Status code :", r.status_code)
    print("Json : ", r.json())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", default=8088,
                        help="Port at which the service will listen on")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enables debug logging")

    args = parser.parse_args()

    test("http://0.0.0.0:{}".format(args.port))
