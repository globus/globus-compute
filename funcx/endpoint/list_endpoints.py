import glob
import os
import json
import texttable as tt


def list_endpoints(args):
    """ List all available endpoints
    """
    table = tt.Texttable()

    headings = ['Endpoint Name', 'Status', 'Endpoint ID']
    table.header(headings)

    config_files = glob.glob('{}/*/config.py'.format(args.config_dir))
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
