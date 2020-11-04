import os


class GlobusFile:
    """The Globus File Class.

    This represents the globus filpath to a file.
    """    
    def __init__(self,
                 endpoint,
                 path,
                 recursive=False):
        """ Initialize the client

        Parameters
        ----------
        endpoint: str
        The endpoint id where the data is located. Required

        path: str
        The path where the data is located on the endpoint. Required

        recursive: boolean
        A boolean indicating whether the data is a directory or a file.
        Default is False (a file).

        """
        self.endpoint = endpoint
        self.path = path
        self.recursive = recursive

    def generate_url(self):
        return f"globus://{self.endpoint}/{self.path}"

    def get_recursive(self):
        return self.recursive
