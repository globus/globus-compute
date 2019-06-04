from funcx_sdk.utils.auth import do_login_flow, make_authorizer, logout
from funcx_sdk.config import (check_logged_in, FUNCX_SERVICE_ADDRESS, CLIENT_ID)

from globus_sdk.base import BaseClient, slash_join
from mdf_toolbox import login, logout

import pickle as pkl
import codecs
import os

_token_dir = os.path.expanduser("~/.funcx/credentials")


class FuncXClient(BaseClient):
    """Main class for interacting with the funcX service

    Holds helper operations for performing common tasks with the funcX service.
    """

    def __init__(self, fx_authorizer=None, http_timeout=None,
                 force_login=False, **kwargs):
        """Initialize the client
        Args:
            fx_authorizer (:class:`GlobusAuthorizer
                            <globus_sdk.authorizers.base.GlobusAuthorizer>`):
                An authorizer instance used to communicate with funcX.
                If ``None``, will be created.
            http_timeout (int): Timeout for any call to service in seconds. (default is no timeout)
            force_login (bool): Whether to force a login to get new credentials.
                A login will always occur if ``fx_authorizer`` 
                are not provided.
        Keyword arguments are the same as for BaseClient.
        """
        if force_login or not fx_authorizer or not search_client:
            fx_scope = "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
            auth_res = login(services=[fx_scope], 
                             app_name="funcX_Client",
                             client_id=CLIENT_ID, clear_old_tokens=force_login,
                             token_dir=_token_dir)
            dlh_authorizer = auth_res['funcx_service']

        super(FuncXClient, self).__init__("funcX", environment='funcx', authorizer=dlh_authorizer,
                                          http_timeout=http_timeout, base_url=FUNCX_SERVICE_ADDRESS,
                                          **kwargs)

    def logout(self):
        """Remove credentials from your local system"""
        logout()

    def get_task_status(self, task_id):
        """Get the status of a funcX task.

        Args:
            task_id (string): UUID of the task
        Returns:
            (dict) status block containing "status" key.
        """

        r = self.get("{task_id}/status".format(task_id=task_id))
        return r.text

    def run(self, inputs, endpoint, func_id, is_async=False, input_type='json'):
        """Initiate an invocation

        Args:
            inputs: Data to be used as input to the function. Can be a string of file paths or URLs
            input_type (string): How to send the data to funcX. Can be "python" (which pickles
                the data), "json" (which uses JSON to serialize the data), or "files" (which
                sends the data as files).
        Returns:
            Reply from the service
        """
        servable_path = 'execute'
        data = {'endpoint': endpoint, 'func': func_id, 'is_async': is_async}

        # Prepare the data to be sent to funcX
        if input_type == 'python':
            data['python'] = codecs.encode(pkl.dumps(inputs), 'base64').decode()
        elif input_type == 'json':
            print(data)
            data['data'] = inputs
        elif input_type == 'files':
            raise NotImplementedError('Files support is not yet implemented')
        else:
            raise ValueError('Input type not recognized: {}'.format(input_type))
        
        # Send the data to funcX
        r = self.post(servable_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data

    def register_endpoint(self, name, description=None):
        """Register an endpoint with the funcX service.

        Args:
            name: str name of the endpoint
            description: str describing the site
        Returns:
            The port to connect to
        """
        registration_path = 'register_endpoint'

        data = {"endpoint_name": name, "description": description}

        r = self.post(registration_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['endpoint_uuid']

    def register_function(self, name, code, entry_point='funcx_handler', description=None):
        """Register a function code with the funcX service.

        Args:
            name: str name of the endpoint
            description: str describing the site
            code: str containing function code
        Returns:
            The name of the function
        """
        registration_path = 'register_function'

        data = {"function_name": name, "function_code": code, "entry_point": entry_point, "description": description}

        r = self.post(registration_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['function_name']
