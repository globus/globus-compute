from funcx.serialize import FuncXSerializer
from funcx.sdk.file import GlobusFile


class Batch:
    """Utility class for creating batch submission in funcX"""

    def __init__(self):
        self.tasks = []
        self.fx_serializer = FuncXSerializer()

    def add(self,
            *args,
            remote_data=None,
            endpoint_id=None,
            function_id=None,
            **kwargs):
        """Add an function invocation to a batch submission

        Parameters
        ----------
        *args : Any
            Args as specified by the function signature
        remote_data : GlobusFile object
            remote data path. Optional
        endpoint_id : uuid str
            Endpoint UUID string. Required
        function_id : uuid str
            Function UUID string. Required
        asynchronous : bool
            Whether or not to run the function asynchronously

        Returns
        -------
        None
        """
        assert endpoint_id is not None, "endpoint_id key-word argument must be set"
        assert function_id is not None, "function_id key-word argument must be set"
        assert isinstance(remote_data, GlobusFile), "Please use GlobusFile to define your remote data"

        ser_args = self.fx_serializer.serialize(args)
        ser_kwargs = self.fx_serializer.serialize(kwargs)
        payload = self.fx_serializer.pack_buffers([ser_args, ser_kwargs])

        data_url = remote_data.generate_url() if remote_data else None
        recursive = remote_data.get_recursive() if remote_data else False

        data = {'endpoint': endpoint_id,
                'function': function_id,
                'payload': payload,
                'data_url': data_url,
                'recursive': recursive,
        }

        self.tasks.append(data)

    def prepare(self):
        """Prepare the payloads to be post to web service in a batch

        Parameters
        ----------

        Returns
        -------
        payloads in dictionary, Dict[str, list]
        """
        data = {
            'tasks': []
        }

        for task in self.tasks:
            new_task = (task['function'],
                        task['endpoint'],
                        task['payload'],
                        task['data_url'],
                        task['recursive'])
            data['tasks'].append(new_task)

        return data
