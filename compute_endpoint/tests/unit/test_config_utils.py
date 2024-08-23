import json
import pathlib
import shlex
import typing as t
import uuid
from unittest import mock

import jinja2
import jsonschema
import pytest
import yaml
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.endpoint.config.utils import (
    RESERVED_USER_CONFIG_TEMPLATE_VARIABLES,
    _validate_user_opts,
    load_user_config_schema,
    load_user_config_template,
    render_config_user_template,
)
from globus_compute_endpoint.endpoint.endpoint import Endpoint

_MOCK_BASE = "globus_compute_endpoint.endpoint.config.utils."


@pytest.fixture
def mock_log():
    with mock.patch(f"{_MOCK_BASE}log") as m:
        yield m


@pytest.fixture(autouse=True)
def use_fs(fs):
    yield


@pytest.fixture
def conf_no_exec():
    yield Config(executors=[])  # empty list to avoid unnecessary network lookup


def test_render_user_config_escape_strings(conf_no_exec):
    template = """
endpoint_setup: {{ setup }}
engine:
    type: {{ engine.type }}
    accelerators:
        {%- for a in engine.accelerators %}
        - {{ a }}
        {% endfor %}"""

    user_opts = {
        "setup": f"my-setup\nallowed_functions:\n    - {uuid.uuid4()}",
        "engine": {
            "type": "GlobusComputeEngine\n    task_status_queue: bad_boy_queue",
            "accelerators": [f"{uuid.uuid4()}\n    mem_per_worker: 100"],
        },
    }
    rendered_str = render_config_user_template(conf_no_exec, template, {}, user_opts)
    loaded = yaml.safe_load(rendered_str)

    assert len(loaded) == 2
    assert len(loaded["engine"]) == 2
    assert loaded["endpoint_setup"] == user_opts["setup"]
    assert loaded["engine"]["type"] == user_opts["engine"]["type"]
    assert loaded["engine"]["accelerators"] == user_opts["engine"]["accelerators"]


@pytest.mark.parametrize(
    "data",
    [
        (True, 10),
        (True, "bar"),
        (True, 10.0),
        (True, ["bar", 10]),
        (True, {"bar": 10}),
        (False, ("bar", 10)),
        (False, {"bar", 10}),
        (False, str),
        (False, Exception),
        (False, locals),
    ],
)
def test_render_user_config_option_types(conf_no_exec, data):
    is_valid, val = data
    template = "foo: {{ foo }}"
    user_opts = {"foo": val}

    if is_valid:
        render_config_user_template(conf_no_exec, template, {}, user_opts)
    else:
        with pytest.raises(ValueError) as pyt_exc:
            render_config_user_template(conf_no_exec, template, {}, user_opts)
        assert "not a valid user config option type" in pyt_exc.exconly()


@pytest.mark.parametrize(
    "data",
    [
        ("{{ foo.__class__ }}", "bar"),
        ("{{ foo.__code__ }}", lambda: None),
        ("{{ foo._priv }}", type("Foo", (object,), {"_priv": "secret"})()),
    ],
)
def test_render_user_config_sandbox(conf_no_exec, data: t.Tuple[str, t.Any]):
    jinja_op, val = data
    template = f"foo: {jinja_op}"
    user_opts = {"foo": val}
    with mock.patch(f"{_MOCK_BASE}_sanitize_user_opts", return_value=user_opts):
        with pytest.raises(jinja2.exceptions.SecurityError):
            render_config_user_template(conf_no_exec, template, {}, user_opts)


@pytest.mark.parametrize(
    "data",
    [
        (True, "-lh"),
        (True, "'-lh'"),
        (True, "'-l' '-h'"),
        (True, "-lh; rm -rf"),
        (True, '-lh && "rm -rf"'),
        (True, '"-lh && "rm -rf"'),
        (True, '-lh && rm -rf"'),
        (True, "\0Do this thing"),
        (True, '\r"-lh && rm -rf"'),
        (True, '\n"-lh && rm -rf"'),
        (True, '"-lh && \\u0015rm -rf"'),
        (True, "-lh\nbad: boy"),
        (False, 10),
        (False, "10"),
    ],
)
def test_render_user_config_shell_escape(conf_no_exec, data: t.Tuple[bool, t.Any]):
    is_valid, option = data
    template = "option: {{ option|shell_escape }}"
    user_opts = {"option": option}
    rendered = render_config_user_template(conf_no_exec, template, {}, user_opts)
    rendered_dict = yaml.safe_load(rendered)

    assert len(rendered_dict) == 1
    rendered_option = rendered_dict["option"]
    if is_valid:
        escaped_option = shlex.quote(option)
        assert f"ls {rendered_option}" == f"ls {escaped_option}"
    else:
        assert rendered_option == option


@pytest.mark.parametrize("schema_exists", (True, False))
def test_render_user_config_apply_schema(conf_no_exec, schema_exists: bool):
    template = "foo: {{ foo }}"
    schema = {}
    if schema_exists:
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "foo": {"type": "string"},
            },
        }

    user_opts = {"foo": "bar"}
    with mock.patch.object(jsonschema, "validate") as mock_validate:
        render_config_user_template(conf_no_exec, template, schema, user_opts)

    if schema_exists:
        assert mock_validate.called
        *_, kwargs = mock_validate.call_args
        assert kwargs["instance"] == user_opts
        assert kwargs["schema"] == schema
    else:
        assert not mock_validate.called


def test_render_config_passes_parent_config(conf_no_exec):
    template = "parent_heartbeat: {{ parent_config.heartbeat_period }}"

    rendered = render_config_user_template(conf_no_exec, template)

    rendered_dict = yaml.safe_load(rendered)
    assert rendered_dict["parent_heartbeat"] == conf_no_exec.heartbeat_period


def test_render_config_passes_user_runtime(conf_no_exec):
    template = "user_python: {{ user_runtime.python_version }}"
    user_runtime = {"python_version": "X.Y.Z"}

    rendered = render_config_user_template(
        conf_no_exec, template, user_runtime=user_runtime
    )

    rendered_dict = yaml.safe_load(rendered)
    assert rendered_dict["user_python"] == user_runtime["python_version"]


@pytest.mark.parametrize(
    "data",
    [
        (True, {"foo": "bar", "nest": {"nested": 10}}),
        (True, {"foo": "bar", "extra": "ok"}),
        (False, {"foo": 10}),
        (False, {"foo": {"nested": "bar"}}),
        (False, {"nest": "nested"}),
        (False, {"nest": {"nested": "blah", "extra": "baddie"}}),
    ],
)
def test_validate_user_config_options(mock_log, data: t.Tuple[bool, dict]):
    is_valid, user_opts = data

    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "foo": {"type": "string"},
            "nest": {
                "type": "object",
                "properties": {"nested": {"type": "number"}},
                "additionalProperties": False,
            },
        },
    }
    if is_valid:
        _validate_user_opts(user_opts, schema)
    else:
        with pytest.raises(jsonschema.ValidationError):
            _validate_user_opts(user_opts, schema)
        assert mock_log.error.called
        a, *_ = mock_log.error.call_args
        assert "user config options are invalid" in str(a)


@pytest.mark.parametrize("schema", ["foo", {"type": "blah"}])
def test_validate_user_config_options_invalid_schema(mock_log, schema):
    user_opts = {"foo": "bar"}
    with pytest.raises(jsonschema.SchemaError):
        _validate_user_opts(user_opts, schema)
    assert mock_log.error.called
    a, *_ = mock_log.error.call_args
    assert "user config schema is invalid" in str(a)


@pytest.mark.parametrize("reserved_word", RESERVED_USER_CONFIG_TEMPLATE_VARIABLES)
def test_validate_user_opts_reserved_words(conf_no_exec, reserved_word):
    with pytest.raises(ValueError) as pyt_exc:
        render_config_user_template(conf_no_exec, {}, user_opts={reserved_word: "foo"})

    assert reserved_word in str(pyt_exc)
    assert "reserved" in str(pyt_exc)


@pytest.mark.parametrize(
    "data", [(True, '{"foo": "bar"}'), (False, '{"foo": "bar", }')]
)
def test_load_user_config_schema(mock_log, data: t.Tuple[bool, str]):
    is_valid, schema_json = data

    conf_dir = pathlib.Path("/")
    template = Endpoint.user_config_schema_path(conf_dir)
    template.write_text(schema_json)

    if is_valid:
        schema = load_user_config_schema(conf_dir)
        assert schema == json.loads(schema_json)
    else:
        with pytest.raises(json.JSONDecodeError):
            load_user_config_schema(conf_dir)
        assert mock_log.error.called
        a, *_ = mock_log.error.call_args
        assert "user config schema is not valid JSON" in str(a)


@pytest.mark.parametrize("ext", (".yaml.j2", ".yaml"))
@pytest.mark.parametrize("ep_name", ("my-ep", "my.j2-ep"))
def test_load_user_config_template_valid_extensions(
    ep_name: str, ext: str, randomstring
):
    conf_dir = pathlib.Path(f"/{randomstring()}/{ep_name}")
    conf_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    template_path = conf_dir / f"user_config_template{ext}"
    template_str = "multi_user: true"
    template_path.write_text(template_str)

    assert load_user_config_template(conf_dir) == (template_str, None)


def test_load_user_config_template_prefer_j2():
    conf_dir = pathlib.Path("/")
    (conf_dir / "user_config_template.yaml").write_text("yaml")
    (conf_dir / "user_config_template.yaml.j2").write_text("j2")
    assert load_user_config_template(conf_dir) == ("j2", None)


@pytest.mark.parametrize(
    "data",
    [
        (True, {"heartbeat": 10}),
        (True, {"heartbeat": 10, "foo": "bar"}),
        (False, {}),
        (False, {"foo": "bar"}),
    ],
)
def test_render_user_config(mock_log, conf_no_exec, data):
    is_valid, user_opts = data
    template = "heartbeat_period: {{ heartbeat }}"

    if is_valid:
        rendered = render_config_user_template(conf_no_exec, template, {}, user_opts)
        rendered_dict = yaml.safe_load(rendered)
        assert rendered_dict["heartbeat_period"] == user_opts["heartbeat"]
    else:
        with pytest.raises(jinja2.exceptions.UndefinedError):
            render_config_user_template(conf_no_exec, template, {}, user_opts)
        a, _k = mock_log.debug.call_args
        assert "Missing required" in a[0]
