from __future__ import annotations

from dataclasses import asdict, dataclass

import yaml


@dataclass
class PathConfiguration:
    """
    :param endpoint_dir: The directory to store endpoint configuration and logs
        The default is $HOME/.globus_compute/<endpoint_name>
    :param endpoint_log: The path to write endpoint logs to.
        The default is <endpoint_dir>/endpoint.log
        See :ref:`endpoint-paths` for more information
    """

    endpoint_dir: str | None = None
    endpoint_log: str | None = None


def _to_yaml(dumper: yaml.SafeDumper, data: PathConfiguration):
    return dumper.represent_mapping("tag:yaml.org,2002:map", asdict(data))


yaml.SafeDumper.add_representer(PathConfiguration, _to_yaml)
