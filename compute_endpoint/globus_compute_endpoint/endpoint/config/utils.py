from __future__ import annotations

import importlib.util
import inspect
import json
import logging
import pathlib

import yaml
from click import ClickException
from packaging.version import Version
from pydantic import ValidationError

from .config import Config
from .model import ConfigModel

log = logging.getLogger(__name__)


def _load_config_py(conf_path: pathlib.Path) -> Config | None:
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


def _read_config_yaml(config_path: pathlib.Path) -> str:
    endpoint_dir = config_path.parent
    endpoint_name = endpoint_dir.name

    try:
        with open(config_path) as f:
            return f.read()
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


def load_config_yaml(config_str: str) -> Config:
    try:
        config_dict = dict(yaml.safe_load(config_str))
    except Exception as err:
        raise ClickException(f"Invalid config syntax: {str(err)}") from err

    try:
        config_schema = ConfigModel(**config_dict)
    except ValidationError as err:
        raise ClickException(str(err)) from err

    config_dict = config_schema.dict(exclude_unset=True)

    try:
        config = Config(**config_dict)
    except Exception as err:
        raise ClickException(str(err)) from err

    # Special case of 'display_name: None' converting to the empty
    #   None instead of the string "None" which was the default
    #   display_name since we started generating yaml configs from
    #   Jun til Jul 2023.
    # This has the side effect of disallowing the display name 'None'
    if config.display_name == "None":
        config.display_name = None
    return config


def get_config(endpoint_dir: pathlib.Path) -> Config:
    config_py_path = endpoint_dir / "config.py"
    config_yaml_path = endpoint_dir / "config.yaml"

    config = _load_config_py(config_py_path)
    if config:
        # Use config.py if present
        # Note that if config.py exists but is invalid,
        # an exception will be raised
        return config

    config_str = _read_config_yaml(config_yaml_path)
    config = load_config_yaml(config_str)
    return config


def render_config_user_template(endpoint_dir: pathlib.Path, user_opts: dict) -> str:
    # Only load package when called by EP manager
    import jinja2

    user_config_path = endpoint_dir / "config_user.yaml"

    template_str = _read_config_yaml(user_config_path)
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = environment.from_string(template_str)

    try:
        return template.render(**user_opts)
    except jinja2.exceptions.UndefinedError as e:
        log.debug(f"Missing required user option(s): {str(e)}")
        raise


def serialize_config(config: Config) -> dict:
    """Converts a Config object into a dict that matches the
    standard YAML config schema.

    E.g.,
    {
        ...
        "display_name": "My Endpoint",
        ...
        "executor": {
            ...
            "provider": {
                ...
                "type": "LocalProvider"
                ...
            }
            ...
        }
    }
    """

    def _prep(val):
        if hasattr(val, "__dict__") and hasattr(val, "__init__"):
            return _to_dict(val)
        try:
            json.dumps(val, allow_nan=False)
            return val
        except (TypeError, ValueError):
            return repr(val)

    def _to_dict(obj):
        res = {"type": type(obj).__name__}
        obj_dict = vars(obj)

        # We only want to include attributes that are
        # configurable as constructor arguments
        sig = inspect.signature(obj.__init__)
        params = list(sig.parameters.keys())

        for key in params:
            try:
                val = obj_dict[key]
            except KeyError:
                continue

            if isinstance(val, (list, tuple)):
                new_list = []
                for item in val:
                    new_list.append(_prep(item))
                res[key] = new_list

            else:
                res[key] = _prep(val)

        return res

    return _to_dict(config)
