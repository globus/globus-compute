from __future__ import annotations

import importlib.util
import inspect
import json
import logging
import pathlib
import re
import shlex
import uuid

import yaml
from click import ClickException
from globus_compute_common.pydantic_v1 import ValidationError

from .config import ManagerEndpointConfig, UserEndpointConfig

log = logging.getLogger(__name__)


RESERVED_USER_CONFIG_TEMPLATE_VARIABLES = (
    "parent_config",
    "user_runtime",
)


def _read_config_file(config_path: pathlib.Path) -> str:
    return config_path.read_text()


def _load_config_py(
    conf_path: pathlib.Path,
) -> UserEndpointConfig | ManagerEndpointConfig | None:
    if not conf_path.exists():
        return None

    try:
        spec = importlib.util.spec_from_file_location("config", conf_path)
        if not (spec and spec.loader):
            raise Exception(f"Unable to import configuration (no spec): {conf_path}")
        config = importlib.util.module_from_spec(spec)
        if not config:
            raise Exception(f"Unable to import configuration (no config): {conf_path}")
        spec.loader.exec_module(config)
        config.config.source_content = _read_config_file(conf_path)
        if not isinstance(config.config, (UserEndpointConfig, ManagerEndpointConfig)):
            tname = type(config.config).__name__
            exp = ", ".join(
                f"`{type(cls).__name__}`"
                for cls in (
                    UserEndpointConfig,
                    ManagerEndpointConfig,
                )  # no hard-code in str
            )
            raise AttributeError(f"Received type `{tname}`; expected one of: {exp}")
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

    except Exception:
        log.exception(
            "Globus Compute v2.0.0 made several non-backwards compatible changes to "
            "the config. Your config might be out of date.  Refer to "
            "https://globus-compute.readthedocs.io/en/latest/endpoints.html#configuring-an-endpoint"  # noqa
        )
        raise


def load_config_yaml(config_str: str) -> UserEndpointConfig | ManagerEndpointConfig:
    try:
        config_dict = dict(yaml.safe_load(config_str))
    except Exception as err:
        raise ClickException(f"Invalid config syntax: {str(err)}") from err

    is_templatable = config_dict.get("multi_user", False) is True
    try:
        ConfigClass: type[UserEndpointConfig | ManagerEndpointConfig]
        if is_templatable:
            from . import BaseEndpointConfigModel, ManagerEndpointConfigModel

            ConfigClass = ManagerEndpointConfig
            config_schema: BaseEndpointConfigModel = ManagerEndpointConfigModel(
                **config_dict
            )
        else:
            from . import UserEndpointConfigModel

            ConfigClass = UserEndpointConfig
            config_schema = UserEndpointConfigModel(**config_dict)
    except ValidationError as err:
        raise ClickException(str(err)) from err

    config_dict = config_schema.dict(exclude_unset=True)

    try:
        config = ConfigClass(**config_dict)
    except Exception as err:
        raise ClickException(str(err)) from err

    # Special case of 'display_name: None' converting to the empty
    #   None instead of the string "None" which was the default
    #   display_name since we started generating yaml configs from
    #   Jun til Jul 2023.
    # This has the side effect of disallowing the display name 'None'
    if config.display_name == "None":
        config.display_name = None

    config.source_content = config_str
    return config


def get_config(
    endpoint_dir: pathlib.Path,
) -> UserEndpointConfig | ManagerEndpointConfig:
    config_py_path = endpoint_dir / "config.py"
    config_yaml_path = endpoint_dir / "config.yaml"

    config = _load_config_py(config_py_path)
    if config:
        # Use config.py if present
        # Note that if config.py exists but is invalid,
        # an exception will be raised
        return config

    try:
        config_str = _read_config_file(config_yaml_path)
    except FileNotFoundError as err:
        endpoint_name = endpoint_dir.name

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
                "\n3. Try again\n"
            )
        raise ClickException(msg) from err

    return load_config_yaml(config_str)


def load_user_config_schema(schema_path: pathlib.Path) -> dict | None:
    if not schema_path.exists():
        return None

    try:
        return json.loads(schema_path.read_text())
    except json.JSONDecodeError:
        log.error(f"\n\nThe user config schema is not valid JSON: {schema_path}\n")
        raise


def _validate_user_opts(user_opts: dict, schema: dict | None) -> None:
    """Validates user config options, optionally against a JSON schema."""

    for reserved_word in RESERVED_USER_CONFIG_TEMPLATE_VARIABLES:
        if reserved_word in user_opts:
            raise ValueError(
                f"'{reserved_word}' is a reserved word"
                " and cannot be passed in via user config"
            )

    if not schema:
        return

    import jsonschema  # Only load package when called by EP manager

    try:
        jsonschema.validate(instance=user_opts, schema=schema)
    except jsonschema.SchemaError:
        log.error("\n\nThe user config schema is invalid\n")
        raise
    except jsonschema.ValidationError:
        log.error("\n\nThe provided user config options are invalid\n")
        raise


def _sanitize_user_opts(data):
    """Prevent YAML injection via special characters.
    Note that this will enforce double quoted YAML strings.
    """
    if isinstance(data, dict):
        return {k: _sanitize_user_opts(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_sanitize_user_opts(v) for v in data]
    elif isinstance(data, str):
        return json.dumps(data)
    elif isinstance(data, (int, float)):
        return data
    else:
        # We do not expect to hit this because user options are passed
        # from the web service as JSON
        raise ValueError(
            f"{type(data).__name__} is not a valid user config option type"
        )


def _shell_escape_filter(val):
    """Returns a shell-escaped version of the argument"""
    if not isinstance(val, str):
        return val

    # We need to escape the command before serializing into JSON
    # because PyYAML will strip the surrounding quotes when the
    # YAML content is loaded
    loaded = json.loads(val)
    if not isinstance(loaded, str):
        return val

    return json.dumps(shlex.quote(loaded))


def load_user_config_template(template_path: pathlib.Path) -> str:
    # Reminder: this method _reads from the filesystem_, so will need appropriate
    # privileges.  Per sc-28360, separate out from the rendering so that we can
    # load the file data into a string before dropping privileges.
    if not template_path.exists():
        file_name = template_path.name
        log.info(f"{file_name}.yaml.j2 does not exist; trying .yaml")
        template_path = pathlib.Path(re.sub(r"\.j2$", "", str(template_path)))
    return _read_config_file(template_path)


def render_config_user_template(
    parent_config: ManagerEndpointConfig,
    user_config_template: str,
    user_config_template_path: pathlib.Path,
    user_config_schema: dict | None = None,
    user_opts: dict | None = None,
    user_runtime: dict | None = None,
) -> str:
    # N.B. Performing rendering, a mildly complicated action, *after*
    # having dropped privileges.  Take security seriously ...

    import jinja2  # Only load package when called by EP manager
    from jinja2.sandbox import SandboxedEnvironment

    _user_opts = user_opts or {}
    _validate_user_opts(_user_opts, user_config_schema)
    _user_opts = _sanitize_user_opts(_user_opts)

    _user_runtime = user_runtime or {}
    _user_runtime = _sanitize_user_opts(_user_runtime)

    user_config_template_dir = user_config_template_path.parent
    try:
        list(user_config_template_dir.iterdir())
    except PermissionError:
        loader = None
        log.debug(
            "User endpoint does not have permissions to load templates"
            f" from {user_config_template_dir}; extends, include, and import"
            " Jinja tags will not be supported"
        )
    else:
        loader = jinja2.FileSystemLoader(user_config_template_dir)

    environment = SandboxedEnvironment(undefined=jinja2.StrictUndefined, loader=loader)
    environment.filters["shell_escape"] = _shell_escape_filter
    template = environment.from_string(user_config_template)

    try:
        return template.render(
            **_user_opts, parent_config=parent_config, user_runtime=_user_runtime
        )
    except jinja2.exceptions.UndefinedError as e:
        log.debug("Missing required user option: %s", e)
        raise
    except jinja2.exceptions.SecurityError as e:
        log.debug("Template tried accessing insecure code: %s", e)
        raise


def serialize_config(config: UserEndpointConfig | ManagerEndpointConfig) -> dict:
    """Converts a configuration object into a dict that matches the standard YAML
    config schema.

    N.B. the output of this method is ignored by the web service's API; the field
         is deprecated since Aug 2024.

    Example structure partial:
    {
        ...
        "display_name": "My Endpoint",
        ...
        "engine": {
            "executor": {
                "provider": {
                    ...
                    "type": "LocalProvider"
                    ...
                }
                ...
            }
            ...
        }
    }
    """

    def _prep(val):
        if hasattr(val, "__dict__") and hasattr(val, "__init__"):
            return _to_dict(val)
        if isinstance(val, uuid.UUID):
            val = str(val)
        try:
            json.dumps(val, allow_nan=False)
            return val
        except (TypeError, ValueError):
            return repr(val)

    def _to_dict(obj):
        res = {"type": type(obj).__name__}

        # We only want to include attributes that are
        # configurable as constructor arguments
        signatures = [
            # abuse knowledge of class structure, knowing that some attributes are
            # stated in the parent signature
            inspect.signature(super(type(obj), obj).__init__),
            inspect.signature(obj.__init__),
        ]
        params = [k for sig in signatures for k in sig.parameters.keys()]

        for param in params:
            if param == "executors":
                continue
            val = getattr(obj, param, 0)
            if val == 0:
                continue

            if isinstance(val, (list, tuple)):
                res[param] = [_prep(item) for item in val]

            else:
                res[param] = _prep(val)

        return res

    return _to_dict(config)
