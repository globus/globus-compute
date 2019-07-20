from funcx.sdk.config import (check_logged_in, FUNCX_SERVICE_ADDRESS, CLIENT_ID, lookup_option)
from funcx.sdk.utils.auth import do_login_flow, make_authorizer, logout
from funcx.sdk.utils.futures import FuncXFuture

from globus_sdk.base import BaseClient, slash_join
from mdf_toolbox import login, logout

import pickle as pkl
import codecs
import json
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
        if force_login or not fx_authorizer:
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
        """Remove credentials from your local system
        """
        logout()

    def get_task_status(self, task_id):
        """Get the status of a funcX task.

        Parameters
        ----------
        task_id : str
            UUID of the task

        Returns
        -------
        dict
            Status block containing "status" key.
        """

        r = self.get("{task_id}/status".format(task_id=task_id))
        return json.loads(r.text)

    def get_local_endpoint(self):
        """Get the local endpoint if it exists.

        Returns:
            (str) the uuid of the endpoint
        -------
        """

        endpoint_uuid = lookup_option("endpoint_uuid")
        return endpoint_uuid

    def run(self, inputs, endpoint, func_id, asynchronous=False, input_type='json', async_poll=5):

        """Initiate an invocation

        Parameters
        ----------
        inputs : list
            Data to be used as input to the function. Can be a string of file paths or URLs
        input_type : str
            How to send the data to funcX. Can be "python" (which pickles
            the data), "json" (which uses JSON to serialize the data), or "files" (which
            sends the data as files).
        endpoint : str
            The uuid of the endpoint
        func_id : str
            The uuid of the function
        asynchronous : bool
            Whether or not to run the function asynchronously
        input_type : str
            Input type to use: json, python, files
        async_poll : float
            How often to poll for task status

        Returns
        -------
        FuncXFuture
            Future representing the task
        """
        servable_path = 'execute'
        data = {'endpoint': endpoint, 'func': func_id}

        # Prepare the data to be sent to funcX
        if input_type == 'python':
            data['python'] = codecs.encode(pkl.dumps(inputs), 'base64').decode()
        elif input_type == 'json':
            data['data'] = inputs
        elif input_type == 'files':
            raise NotImplementedError('Files support is not yet implemented')
        else:
            raise ValueError('Input type not recognized: {}'.format(input_type))
        
        # Send the data to funcX
        r = self.post(servable_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        task_id = None
        try:
            task_id = r['task_id']
        except:
            pass

        # Create a future to deal with the result
        funcx_future = FuncXFuture(self, task_id, async_poll)

        if not asynchronous:
            return funcx_future.result()

        # Return the result
        return funcx_future

    def register_endpoint(self, name, endpoint_uuid, description=None):
        """Register an endpoint with the funcX service.

        Parameters
        ----------
        name : str
            Name of the endpoint
        endpoint_uuid : str
                The uuid of the endpoint
        description : str
            Description of the endpoint

        Returns
        -------
        int
            The uuid of the endpoint
        """
        registration_path = 'register_endpoint'

        data = {"endpoint_name": name, "endpoint_uuid": endpoint_uuid, "description": description}

        r = self.post(registration_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['endpoint_uuid']

    def get_containers(self, name, description=None):
        """Register a DLHub endpoint with the funcX service and get the containers to launch.

        Parameters
        ----------
        name : str
            Name of the endpoint
        description : str
            Description of the endpoint

        Returns
        -------
        int
            The port to connect to and a list of containers
        """
        registration_path = 'get_containers'

        data = {"endpoint_name": name, "description": description}

        r = self.post(registration_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['endpoint_uuid'], r.data['endpoint_containers']

    def get_container(self, container_uuid, container_type):
        """Get the details of a container for staging it locally.

        Parameters
        ----------
        container_uuid : str
            UUID of the container in question
        container_type : str
            The type of containers that will be used (Singularity, Shifter, Docker)

        Returns
        -------
        dict
            The details of the containers to deploy
        """
        container_path = f'containers/{container_uuid}/{container_type}'

        r = self.get(container_path)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['container']

    def register_container(self, name, location, description, container_type):
        """Register a container with the funcX service.

        Parameters
        ----------
        name : str
            A name for the container
        location : str
            The location of the container (e.g., its docker url)
        description : str
            A description to associate with the container
        container_type : str
            The type of containers that will be used (Singularity, Shifter, Docker)

        Returns
        -------
        str
            The id of the container
        """
        container_path = f'containers'

        payload = {'name': name, 'location': location, 'description': description, 'type': container_type}

        r = self.post(container_path, json_body=payload)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['container_id']

    def register_function(self, name, code, entry_point='funcx_handler', container=None, description=None):
        """Register a function code with the funcX service.

        Parameters
        ----------
        name : str
            Name of the endpoint
        description : str
            Description of the file
        code : str
            Function code
        entry_point : str
            The entry point (function name) of the function
        container : str
            The uuid of the container to run this function in
        description : str
            A description of the container

        Returns
        -------
        str
            The name of the function
        """
        registration_path = 'register_function'

        data = {"function_name": name, "function_code": code, "entry_point": entry_point,
                "container": container, "description": description}

        r = self.post(registration_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['function_uuid']
