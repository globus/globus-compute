class ContainerSpec:
    def __init__(
        self,
        name=None,
        description=None,
        apt=None,
        pip=None,
        conda=None,
        payload_url=None,
    ):
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
