from __future__ import annotations

import importlib.util
import logging
import pathlib

import yaml
from click import ClickException
from packaging.version import Version
from pydantic import ValidationError

from .config import Config
from .model import ConfigModel

log = logging.getLogger(__name__)


def read_config_py(endpoint_dir: pathlib.Path) -> Config | None:
    conf_path = endpoint_dir / "config.py"

    if not conf_path.exists():
        return None

    try:
        from funcx_endpoint.version import VERSION

        if Version(VERSION) < Version("2.0.0"):
            msg = (
                "To avoid compatibility issues with Globus Compute, please uninstall "
                "funcx-endpoint or upgrade funcx-endpoint to >=2.0.0. Note that the "
                "funcx-endpoint package is now deprecated."
            )
            raise ClickException(msg)
    except ModuleNotFoundError:
        pass

    try:
        spec = importlib.util.spec_from_file_location("config", conf_path)
        if not (spec and spec.loader):
            raise Exception(f"Unable to import configuration (no spec): {conf_path}")
        config = importlib.util.module_from_spec(spec)
        if not config:
            raise Exception(f"Unable to import configuration (no config): {conf_path}")
        spec.loader.exec_module(config)
        return config.config

    except FileNotFoundError as err:
        msg = (
            f"{err}"
            "\n\nUnable to find required configuration file; has the configuration"
            "\ndirectory been corrupted?"
        )
        raise ClickException(msg) from err

    except AttributeError as err:
        msg = (
            f"{err}"
            "\n\nFailed to find expected data structure in configuration file."
            "\nHas the configuration file been corrupted or modified incorrectly?\n"
        )
        raise ClickException(msg) from err

    except ModuleNotFoundError as err:
        # Catch specific error when old config.py references funcx_endpoint
        if "No module named 'funcx_endpoint'" in err.msg:
            msg = (
                f"{conf_path} contains import statements from a previously "
                "configured endpoint that uses the (deprecated) "
                "funcx-endpoint library. Please update the imports to reference "
                "globus_compute_endpoint.\n\ni.e.\n"
                "    from funcx_endpoint.endpoint.utils.config -> "
                "from globus_compute_endpoint.endpoint.config\n"
                "    from funcx_endpoint.executors -> "
                "from globus_compute_endpoint.executors\n"
                "\n"
                "You can also use the command "
                "`globus-compute-endpoint update_funcx_config [endpoint_name]` "
                "to update them\n"
            )
            raise ClickException(msg) from err
        else:
            log.exception(err.msg)
            raise

    except Exception:
        log.exception(
            "Globus Compute v2.0.0 made several non-backwards compatible changes to "
            "the config. Your config might be out of date. "
            "Refer to "
            "https://funcx.readthedocs.io/en/latest/endpoints.html#configuring-funcx"
        )
        raise


def read_config_yaml(endpoint_dir: pathlib.Path) -> dict:
    config_path = endpoint_dir / "config.yaml"
    endpoint_name = endpoint_dir.name

    try:
        with open(config_path) as f:
            return dict(yaml.safe_load(f))
    except FileNotFoundError as err:
        if endpoint_dir.exists():
            msg = (
                f"{err}"
                "\n\nUnable to find required configuration file; has the configuration"
                "\ndirectory been corrupted?"
            )
        else:
            configure_command = "globus-compute-endpoint configure"
            if endpoint_name != "default":
                configure_command += f" {endpoint_name}"
            msg = (
                f"{err}"
                f"\n\nEndpoint '{endpoint_name}' is not configured!"
                "\n1. Please create a configuration template with:"
                f"\n\t{configure_command}"
                "\n2. Update the configuration"
                "\n3. Start the endpoint\n"
            )
        raise ClickException(msg) from err
    except Exception as err:
        raise ClickException(f"Invalid syntax in {config_path}: {str(err)}") from err


def get_config(endpoint_dir: pathlib.Path) -> Config:
    config = read_config_py(endpoint_dir)
    if config:
        # Use config.py if present
        # Note that if config.py exists but is invalid,
        # an exception will be raised
        return config

    config_dict = read_config_yaml(endpoint_dir)

    try:
        config_schema = ConfigModel(**config_dict)
    except ValidationError as err:
        raise ClickException(str(err)) from err

    config_dict = config_schema.dict(exclude_unset=True)

    try:
        config = Config(**config_dict)
    except Exception as err:
        raise ClickException(str(err)) from err

    return config
