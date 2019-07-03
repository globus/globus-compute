from bottle import post, run, request
import cherrypy
import argparse
import json
import uuid
import sys

@post('/register')
def register():
    """ Register an endpoint request

    1. Start an executor client object corresponding to the endpoint
    2. Pass connection info back as a json response.
    """

    print("Request: ", request)
    print(json.load(request.body))
    endpoint_details = json.load(request.body)
    print(endpoint_details)

    return {'endpoint_id': str(uuid.uuid4()),
            'python_v': "{}.{}.{}".format(sys.version_info.major,
                                          sys.version_info.minor,
                                          sys.version_info.micro),
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", default=8088,
                        help="Port at which the service will listen on")
    parser.add_argument("-c", "--config", default=None,
                        help="Config file")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enables debug logging")

    args = parser.parse_args()

    run(host='localhost', port=int(args.port), debug=True)
