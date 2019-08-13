import codecs
import json
import os
import logging
import pickle as pkl

from globus_sdk.base import BaseClient, slash_join
from mdf_toolbox import login, logout
from funcx.sdk.utils.auth import do_login_flow, make_authorizer, logout
from funcx.serialize import FuncXSerializer
# from funcx.sdk.utils.futures import FuncXFuture

logger = logging.getLogger(__name__)

class FuncXClient(BaseClient):
    """Main class for interacting with the funcX service

    Holds helper operations for performing common tasks with the funcX service.
    """

    TOKEN_DIR = os.path.expanduser("~/.funcx/credentials")
    CLIENT_ID = '4cf29807-cf21-49ec-9443-ff9a3fb9f81c'
    # FUNCX_SERVICE_ADDRESS = "https://funcx.org/api/v1"
    FUNCX_SERVICE_ADDRESS = "https://dev.funcx.org/api/v1"

    def __init__(self, http_timeout=None, funcx_home=os.path.join('~', '.funcx'),
                 force_login=False, fx_authorizer=None, **kwargs):
        """ Initialize the client

        Parameters
        ----------
        http_timeout: int
        Timeout for any call to service in seconds.
        Default is no timeout

        force_login: bool
        Whether to force a login to get new credentials.

        fx_authorizer:class:`GlobusAuthorizer <globus_sdk.authorizers.base.GlobusAuthorizer>`:
        A custom authorizer instance to communicate with funcX.
        Default: ``None``, will be created.

        Keyword arguments are the same as for BaseClient.
        """
        self.ep_registration_path = 'register_endpoint_2'
        self.funcx_home = os.path.expanduser(funcx_home)


        if force_login or not fx_authorizer:
            fx_scope = "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
            auth_res = login(services=[fx_scope],
                             app_name="funcX_Client",
                             client_id=self.CLIENT_ID,
                             clear_old_tokens=force_login,
                             token_dir=self.TOKEN_DIR)
            dlh_authorizer = auth_res['funcx_service']

        super(FuncXClient, self).__init__("funcX",
                                          environment='funcx',
                                          authorizer=dlh_authorizer,
                                          http_timeout=http_timeout,
                                          base_url=self.FUNCX_SERVICE_ADDRESS,
                                          **kwargs)
        self.fx_serializer = FuncXSerializer()

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

    def run(self, *args, endpoint_id=None, function_id=None, asynchronous=False, **kwargs):
        """Initiate an invocation

        Parameters
        ----------
        *args : Any
            Args as specified by the function signature
        endpoint_id : uuid str
            Endpoint UUID string. Required
        function_id : uuid str
            Function UUID string. Required
        asynchronous : bool
            Whether or not to run the function asynchronously

        Returns
        -------
        task_id : str
        UUID string that identifies the task
        """
        servable_path = 'submit'
        assert endpoint_id is not None, "endpoint_id key-word argument must be set"
        assert function_id is not None, "function_id key-word argument must be set"

        ser_args = self.fx_serializer.serialize(args)
        ser_kwargs = self.fx_serializer.serialize(kwargs)
        payload = self.fx_serializer.pack_buffers([ser_args, ser_kwargs])

        data = {'endpoint': endpoint_id,
                'func': function_id,
                'payload': payload,
                'is_async': asynchronous}

        # Send the data to funcX
        r = self.post(servable_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        if 'task_uuid' not in r:
            raise MalformedResponse(r)

        """
        Create a future to deal with the result
        funcx_future = FuncXFuture(self, task_id, async_poll)

        if not asynchronous:
            return funcx_future.result()

        # Return the result
        return funcx_future
        """
        return r['task_uuid']


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
        A dict
            {'endopoint_id' : <>,
             'address' : <>,
             'client_ports': <>}
        """
        data = {"endpoint_name": name, "endpoint_uuid": endpoint_uuid, "description": description}

        r = self.post(self.ep_registration_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        logger.info("Data returned : {}".format(r.data))
        return r.data

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

    def register_function(self, function, function_name=None, container_uuid=None, description=None):
        """Register a function code with the funcX service.

        Parameters
        ----------
        function : Python Function
            The function to be registered for remote execution

        function_name : str
            The entry point (function name) of the function. Default: None

        container_uuid : str
            Container UUID from registration with funcX

        description : str
            Description of the file

        Returns
        -------
        function uuid : str
            UUID identifier for the registered function
        """
        registration_path = 'register_function'

        serialized_fn = self.fx_serializer.serialize(function)
        packed_code = self.fx_serializer.pack_buffers([serialized_fn])

        data = {"function_name": function.__name__,
                "function_code": packed_code,
                "container_uuid": container_uuid,
                "entry_point": function_name if function_name else function.__name__,
                "description": description}

        logger.info("Registering function : {}".format(data))

        r = self.post(registration_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['function_uuid']
