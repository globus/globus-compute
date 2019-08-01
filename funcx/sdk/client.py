import codecs
import os
import logging
import pickle as pkl

from globus_sdk.base import BaseClient, slash_join
from mdf_toolbox import login, logout
from funcx.sdk.utils.auth import do_login_flow, make_authorizer, logout

logger = logging.getLogger(__name__)

class FuncXClient(BaseClient):
    """Main class for interacting with the funcX service

    Holds helper operations for performing common tasks with the funcX service.
    """

    TOKEN_DIR = os.path.expanduser("~/.funcx/credentials")
    CLIENT_ID = '4cf29807-cf21-49ec-9443-ff9a3fb9f81c'
    # FUNCX_SERVICE_ADDRESS = "https://funcx.org/api/v1"
    FUNCX_SERVICE_ADDRESS = "https://dev.funcx.org/api/v1"

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
        self.ep_registration_path = 'register_endpoint_2'

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
        return r.text

    def run(self, inputs, endpoint, func_id, asynchronous=False, input_type='json'):

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

        Returns
        -------
        dict
            Reply from the service
        """
        servable_path = 'execute'
        data = {'endpoint': endpoint, 'func': func_id, 'is_async': asynchronous}

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

        # Return the result
        return r.data

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

    def get_container(self, container_id, container_type):
        """Get the details of a container for staging it locally.

        Parameters
        ----------
        container_id : str
            UUID of the container in question
        container_type : str
            The type of containers that will be used (Singularity, Shifter, Docker)

        Returns
        -------
        dict
            The details of the containers to deploy
        """
        container_path = f'containers/{container_id}/{container_type}'

        r = self.get(container_path)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['container']

    def register_function(self, name, code, entry_point='funcx_handler', description=None):
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

        Returns
        -------
        str
            The name of the function
        """
        registration_path = 'register_function'

        data = {"function_name": name, "function_code": code, "entry_point": entry_point, "description": description}

        r = self.post(registration_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['function_uuid']
