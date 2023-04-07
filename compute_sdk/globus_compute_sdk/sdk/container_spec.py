from __future__ import annotations

import re


class ContainerSpec:
    # RegEx copied from https://ihateregex.io/expr/semver/
    semver_regex = re.compile(
        r"^(0|[1-9]\d*)\.(0|[1-9]\d*)(\.(0|[1-9]\d*))?(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"  # noqa E501
    )

    def __init__(
        self,
        name: str | None = None,
        description: str | None = None,
        apt: list[str] | None = None,
        pip: list[str] | None = None,
        conda: list[str] | None = None,
        python_version: str = "3.7",  # This is the default for repo2docker
        payload_url: str | None = None,
    ):
        """
        Construct a container spec to pass to service for build operation.

        Parameters
        ----------
        name : str
            Name of this container to be used inside Globus Compute
        description : str
            Description of the container inside Globus Compute
        apt : List[str]
            List of Ubuntu library packages to install in container
        pip : List[str]
            List of Python libraries to install from pypi
        conda : List[str]
            List of Conda packages to install
        python_version : str
            Version of Python to build into image
        payload_url : str
            GitHub repo or publicly readable zip file to copy into container
        """
        if not self.semver_regex.match(python_version):
            raise ValueError("Python version must be a valid semantic version")

        self.name = name
        self.description = description
        self.apt = apt if apt else []
        self.pip = pip if pip else []
        self.conda = conda if conda else []

        # Specify the python version in the conda environment. Don't mess with a
        # user-provided setting in their conda environment if there
        python_in_env = any(s for s in self.conda if s.startswith("python="))

        if not python_in_env:
            self.conda.append(f"python={python_version}")

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
