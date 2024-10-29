from dataclasses import asdict, dataclass

import yaml


@dataclass
class PamConfiguration:
    """
    :param enable: Whether to initiate a PAM session for each UEP start request.

    :param service_name: What PAM service name with which to initialize the PAM
        session.  If a particular MEP has different requirements, define those PAM
        requirements in ``/etc/pam.d/``, and specify the service name with this field.

        See :ref:`MEP § PAM <pam>` for more information
    """

    enable: bool = True
    service_name: str = "globus-compute-endpoint"


def _to_yaml(dumper: yaml.SafeDumper, data: PamConfiguration):
    return dumper.represent_mapping("tag:yaml.org,2002:map", asdict(data))


yaml.SafeDumper.add_representer(PamConfiguration, _to_yaml)
