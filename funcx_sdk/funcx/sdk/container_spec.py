import json


class ContainerSpec:

    def __init__(self, name=None,
                 description=None,
                 apt=[],
                 pip=[],
                 conda=[]):
        self.name = name
        self.description = description
        self.apt = apt
        self.pip = pip
        self.conda = conda

    def to_json(self):
        return {
            "name": self.name,
            "description": self.description,
            "apt": self.apt,
            "pip": self.pip,
            "conda": self.conda
        }

