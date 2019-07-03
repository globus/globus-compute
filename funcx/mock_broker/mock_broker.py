import bottle
from bottle import post, run, request, app
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
    print("foo: ", request.app.ep_mapping)
    print(json.load(request.body))
    endpoint_details = json.load(request.body)
    print(endpoint_details)


    endpoint_id = str(uuid.uuid4())
    ret_package = {'endpoint_id': endpoint_id,
                   'task_url': '',
                   'result_url': '',
                   'command_port': ''}
    print("Ep_id: ", endpoint_id)
    request.app.ep_mapping[endpoint_id] = ret_package
    return ret_package


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", default=8088,
                        help="Port at which the service will listen on")
    parser.add_argument("-c", "--config", default=None,
                        help="Config file")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enables debug logging")

    args = parser.parse_args()
    app = bottle.default_app()
    app.ep_mapping = {}

    run(host='localhost', app=app, port=int(args.port), debug=True)
