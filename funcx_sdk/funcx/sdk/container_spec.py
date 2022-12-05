from __future__ import annotations


class ContainerSpec:
    def __init__(
        self,
        name: str | None = None,
        description: str | None = None,
        apt: list[str] | None = None,
        pip: list[str] | None = None,
        conda: list[str] | None = None,
        payload_url: str | None = None,
    ):
        """
        Construct a container spec to pass to service for build operation.

        Parameters
        ----------
        name : str
            Name of this container to be used inside funcx
        description : str
            Description of the container inside funcx
        apt : List[str]
            List of Ubuntu library packages to install in container
        pip : List[str]
            List of Python libraries to install from pypi
        conda : List[str]
            List of Conda packages to install
        payload_url : str
            GitHub repo or publicly readable zip file to copy into container
        """
        self.name = name
        self.description = description
        self.apt = apt if apt else []
        self.pip = pip if pip else []
        self.conda = conda if conda else []
        self.payload_url = payload_url

    def to_json(self):
        return {
            "name": self.name,
            "description": self.description,
            "apt": self.apt,
            "pip": self.pip,
            "conda": self.conda,
            "payload_url": self.payload_url,
        }
