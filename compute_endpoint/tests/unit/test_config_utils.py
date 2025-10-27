import inspect
import json
import os
import pathlib
import shlex
import typing as t
import uuid
from unittest import mock

import jinja2
import jinja2.sandbox
import jsonschema
import pytest
import yaml
from click import ClickException
from globus_compute_endpoint.endpoint.config import (
    ManagerEndpointConfig,
    UserEndpointConfig,
)
from globus_compute_endpoint.endpoint.config.utils import (
    RESERVED_USER_CONFIG_TEMPLATE_VARIABLES,
    _validate_user_opts,
    get_config,
    load_config_yaml,
    load_user_config_schema,
    load_user_config_template,
    render_config_user_template,
)
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_endpoint.endpoint.identity_mapper import MappedPosixIdentity
from tests.conftest import randomstring_impl
from tests.unit.conftest import known_manager_config_opts, known_user_config_opts

_MOCK_BASE = "globus_compute_endpoint.endpoint.config.utils."


@pytest.fixture
def mock_log():
    with mock.patch(f"{_MOCK_BASE}log") as m:
        yield m


@pytest.fixture(autouse=True)
def use_fs(fs):
    yield


@pytest.fixture
def mapped_ident(mocker):
    mocker.patch.object(os, "getgrouplist", return_value=["testgroup1", "testgroup2"])

    globus_id = str(uuid.uuid4())
    local_user_record = mock.Mock(pw_name="testuser", pw_uid=1000, pw_gid=1001)

    yield MappedPosixIdentity(
        local_user_record=local_user_record,
        globus_identity_candidates={globus_id: [local_user_record.pw_name]},
        matched_identity=globus_id,
    )


@pytest.fixture
def conf_no_exec():
    # empty so as to avoid unnecessary network lookup
    yield UserEndpointConfig(executors=())


def _get_cls_kwds(cls) -> set[str]:
    fas = (inspect.getfullargspec(c.__init__) for c in cls.__mro__)

    return {k for f in fas for k in f.kwonlyargs}


def test_config_opts_accounted_for_in_tests():
    kwds = _get_cls_kwds(UserEndpointConfig)
    kwds.remove("multi_user")  # special case deprecated argument
    assert set(known_user_config_opts) == kwds
    kwds = _get_cls_kwds(ManagerEndpointConfig)
    kwds.remove("multi_user")  # special case deprecated argument
    kwds.remove("force_mu_allow_same_user")  # special case deprecated argument
    assert set(known_manager_config_opts) == kwds


def test_extra_opts_disallowed():
    conf = {"does_not_exist": True, "engine": {"type": "ThreadPoolEngine"}}
    serde_yaml = yaml.safe_dump(conf)
    with pytest.raises(ClickException) as pyt_e:
        load_config_yaml(serde_yaml)
    assert "does_not_exist" in str(pyt_e.value)

    conf = {"does_not_exist": True, "multi_user": True}
    serde_yaml = yaml.safe_dump(conf)
    with pytest.raises(ClickException) as pyt_e:
        load_config_yaml(serde_yaml)
    assert "does_not_exist" in str(pyt_e.value)


def test_load_user_endpoint_config_minimal():
    conf = {"engine": {"type": "ThreadPoolEngine"}}
    serde_yaml = yaml.safe_dump(conf)
    conf = load_config_yaml(serde_yaml)
    assert isinstance(conf, UserEndpointConfig)


def test_load_user_endpoint_config_full(get_random_of_datatype):
    conf = {
        kw: get_random_of_datatype(tval) for kw, tval in known_user_config_opts.items()
    }
    conf.pop("executors")
    conf["engine"] = {"type": "ThreadPoolEngine"}
    serde_yaml = yaml.safe_dump(conf)
    conf = load_config_yaml(serde_yaml)
    assert isinstance(conf, UserEndpointConfig)


def test_load_manager_endpoint_config_minimal():
    conf = {}
    serde_yaml = yaml.safe_dump(conf)
    conf = load_config_yaml(serde_yaml)
    assert isinstance(conf, ManagerEndpointConfig)


def test_load_manager_endpoint_config_full(get_random_of_datatype):
    conf = {
        kw: get_random_of_datatype(tval)
        for kw, tval in known_manager_config_opts.items()
    }
    serde_yaml = yaml.safe_dump(conf)
    conf = load_config_yaml(serde_yaml)
    assert isinstance(conf, ManagerEndpointConfig)


def test_render_user_config_escape_strings(
    conf_no_exec, mapped_ident: MappedPosixIdentity
):
    template = """
endpoint_setup: {{ setup }}
display_name: {{ user_runtime.python.version}}
engine:
    type: {{ engine.type }}
    accelerators:
        {%- for a in engine.accelerators %}
        - {{ a }}
        {% endfor %}
    """

    user_opts = {
        "setup": f"my-setup\nallowed_functions:\n    - {uuid.uuid4()}",
        "engine": {
            "type": "GlobusComputeEngine\n    task_status_queue: bad_boy_queue",
            "accelerators": [f"{uuid.uuid4()}\n    mem_per_worker: 100"],
        },
    }
    user_runtime = {"python": {"version": "3.13.7\n    bar: baz"}}
    rendered_str = render_config_user_template(
        conf_no_exec,
        template,
        pathlib.Path("/"),
        mapped_ident,
        {},
        user_opts,
        user_runtime,
    )
    loaded = yaml.safe_load(rendered_str)

    assert len(loaded) == 3
    assert len(loaded["engine"]) == 2
    assert loaded["endpoint_setup"] == user_opts["setup"]
    assert loaded["display_name"] == user_runtime["python"]["version"]
    assert loaded["engine"]["type"] == user_opts["engine"]["type"]
    assert loaded["engine"]["accelerators"] == user_opts["engine"]["accelerators"]


def test_render_user_config_handles_null_user_values(
    conf_no_exec, mapped_ident: MappedPosixIdentity
):
    template = """
endpoint_setup: {{ some_var }}
display_name: {{ user_runtime.some_var }}
    """

    user_opts = {"some_var": None}
    user_runtime = {"some_var": None}
    rendered_str = render_config_user_template(
        conf_no_exec,
        template,
        pathlib.Path("/"),
        mapped_ident,
        {},
        user_opts,
        user_runtime,
    )
    loaded = yaml.safe_load(rendered_str)

    assert len(loaded) == 2
    assert loaded["endpoint_setup"] is None
    assert loaded["display_name"] is None


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
def test_render_user_config_option_types(
    conf_no_exec, data, mapped_ident: MappedPosixIdentity
):
    is_valid, val = data
    template = "foo: {{ foo }}"
    user_opts = {"foo": val}

    if is_valid:
        render_config_user_template(
            conf_no_exec, template, pathlib.Path("/"), mapped_ident, {}, user_opts
        )
    else:
        with pytest.raises(ValueError) as pyt_exc:
            render_config_user_template(
                conf_no_exec, template, pathlib.Path("/"), mapped_ident, {}, user_opts
            )
        assert "not a valid user config option type" in pyt_exc.exconly()


@pytest.mark.parametrize(
    "data",
    [
        ("{{ foo.__class__ }}", "bar"),
        ("{{ foo.__code__ }}", lambda: None),
        ("{{ foo._priv }}", type("Foo", (object,), {"_priv": "secret"})()),
    ],
)
def test_render_user_config_sandbox(
    conf_no_exec, data: t.Tuple[str, t.Any], mapped_ident: MappedPosixIdentity
):
    jinja_op, val = data
    template = f"foo: {jinja_op}"
    user_opts = {"foo": val}
    with mock.patch(f"{_MOCK_BASE}_sanitize_user_opts", return_value=user_opts):
        with pytest.raises(jinja2.exceptions.SecurityError):
            render_config_user_template(
                conf_no_exec, template, pathlib.Path("/"), mapped_ident, {}, user_opts
            )


@mock.patch.object(jinja2.sandbox, "SandboxedEnvironment")
@mock.patch.object(jinja2, "FileSystemLoader")
def test_render_user_config_environment_loader(
    mock_loader, mock_env, conf_no_exec, mapped_ident: MappedPosixIdentity
):
    template_dir = pathlib.Path("templates/")
    template_dir.mkdir(parents=True, exist_ok=True)
    template_path = template_dir / "user_config_template.yaml"
    template_str = "foo: bar"
    template_path.write_text(template_str)

    template_dir.chmod(0o700)
    render_config_user_template(conf_no_exec, template_str, template_path, mapped_ident)

    mock_loader.assert_called_once_with(template_dir)
    mock_env.assert_called_once_with(
        undefined=jinja2.StrictUndefined, loader=mock_loader.return_value
    )


@mock.patch("jinja2.sandbox.SandboxedEnvironment")
@mock.patch("jinja2.FileSystemLoader")
def test_render_user_config_environment_loader_no_permissions(
    mock_loader, mock_env, conf_no_exec, mock_log, mapped_ident: MappedPosixIdentity
):
    template_dir = pathlib.Path("templates/")
    template_dir.mkdir(parents=True, exist_ok=True)
    template_path = template_dir / "user_config_template.yaml"
    template_str = "foo: bar"
    template_path.write_text(template_str)

    template_dir.chmod(0o000)
    render_config_user_template(conf_no_exec, template_str, template_path, mapped_ident)

    assert mock_log.debug.called
    a, *_ = mock_log.debug.call_args
    assert "User endpoint does not have permissions to load templates" in str(a)

    mock_loader.assert_not_called()
    mock_env.assert_called_once_with(undefined=jinja2.StrictUndefined, loader=None)


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
def test_render_user_config_shell_escape(
    conf_no_exec, data: t.Tuple[bool, t.Any], mapped_ident: MappedPosixIdentity
):
    is_valid, option = data
    template = "option: {{ option|shell_escape }}"
    user_opts = {"option": option}
    rendered = render_config_user_template(
        conf_no_exec, template, pathlib.Path("/"), mapped_ident, {}, user_opts
    )
    rendered_dict = yaml.safe_load(rendered)

    assert len(rendered_dict) == 1
    rendered_option = rendered_dict["option"]
    if is_valid:
        escaped_option = shlex.quote(option)
        assert f"ls {rendered_option}" == f"ls {escaped_option}"
    else:
        assert rendered_option == option


@pytest.mark.parametrize("schema_exists", (True, False))
def test_render_user_config_apply_schema(
    conf_no_exec, schema_exists: bool, mapped_ident: MappedPosixIdentity
):
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
        render_config_user_template(
            conf_no_exec, template, pathlib.Path("/"), mapped_ident, schema, user_opts
        )

    if schema_exists:
        assert mock_validate.called
        *_, kwargs = mock_validate.call_args
        assert kwargs["instance"] == user_opts
        assert kwargs["schema"] == schema
    else:
        assert not mock_validate.called


def test_render_config_passes_parent_config(
    conf_no_exec, mapped_ident: MappedPosixIdentity
):
    template = "parent_heartbeat: {{ parent_config.heartbeat_period }}"

    rendered = render_config_user_template(
        conf_no_exec, template, pathlib.Path("/"), mapped_ident
    )

    rendered_dict = yaml.safe_load(rendered)
    assert rendered_dict["parent_heartbeat"] == conf_no_exec.heartbeat_period


def test_render_config_passes_user_runtime(
    conf_no_exec, mapped_ident: MappedPosixIdentity
):
    template = "user_python: {{ user_runtime.python.version }}"
    user_runtime = {"python": {"version": "X.Y.Z"}}

    rendered = render_config_user_template(
        conf_no_exec,
        template,
        pathlib.Path("/"),
        mapped_ident,
        user_runtime=user_runtime,
    )

    rendered_dict = yaml.safe_load(rendered)
    assert rendered_dict["user_python"] == user_runtime["python"]["version"]


def test_render_config_passes_mapped_identity(mocker, conf_no_exec):
    mock_struct_passwd = mock.Mock(
        pw_name="testuser",
        pw_uid=1000,
        pw_gid=1001,
        pw_gecos="mocker:x:1000:1000::/home/mocker:/bin/sh",
        pw_dir="/home/mocker",
    )
    mock_matched_identity = str(uuid.uuid4())
    mock_candidates = [{mock_matched_identity: ["testuser"]}]
    mock_groups = [1001, 1004]
    mocker.patch.object(os, "getgrouplist", return_value=mock_groups)

    mapped_ident = MappedPosixIdentity(
        local_user_record=mock_struct_passwd,
        globus_identity_candidates=mock_candidates,
        matched_identity=mock_matched_identity,
    )
    template = (
        "uname: {{ mapped_identity.local.uname }}\n"
        "uid: {{ mapped_identity.local.uid }}\n"
        "gid: {{ mapped_identity.local.gid }}\n"
        "groups: {{ mapped_identity.local.groups }}\n"
        "gecos: {{ mapped_identity.local.gecos }}\n"
        "home_dir: {{ mapped_identity.local.dir }}\n"
        "globus_id: {{ mapped_identity.globus.id }}"
    )
    rendered = render_config_user_template(
        conf_no_exec, template, pathlib.Path("/"), mapped_ident
    )

    rendered_dict = yaml.safe_load(rendered)
    assert rendered_dict["uname"] == mock_struct_passwd.pw_name
    assert rendered_dict["uid"] == mock_struct_passwd.pw_uid
    assert rendered_dict["gid"] == mock_struct_passwd.pw_gid
    assert rendered_dict["gecos"] == mock_struct_passwd.pw_gecos
    assert rendered_dict["home_dir"] == mock_struct_passwd.pw_dir
    assert rendered_dict["groups"] == mock_groups
    assert rendered_dict["globus_id"] == mock_matched_identity


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
def test_validate_user_opts_reserved_words(
    conf_no_exec, reserved_word, mapped_ident: MappedPosixIdentity
):
    with pytest.raises(ValueError) as pyt_exc:
        render_config_user_template(
            conf_no_exec,
            "",
            mock.MagicMock(),
            mapped_ident,
            user_opts={reserved_word: "foo"},
        )

    assert reserved_word in str(pyt_exc)
    assert "reserved" in str(pyt_exc)


@pytest.mark.parametrize("conftext", ("display_name: test-name", "", None))
def test_get_config_missing_ok(fs, conftext):
    conf_dir = pathlib.Path("/")
    if conftext is not None:
        (conf_dir / "config.yaml").write_text(conftext)

    conf = get_config(conf_dir)  # basically, not exploding is the test
    assert conftext and conf.display_name == "test-name" or conf.display_name is None


@pytest.mark.parametrize("dirname", (randomstring_impl(),))
def test_get_config_dir_not_exist(fs, dirname):
    conf_dir = pathlib.Path(f"/{dirname}")

    with pytest.raises(ClickException) as pyt_e:
        get_config(conf_dir)

    assert str(conf_dir / "config.yaml") in str(pyt_e.value)
    assert f"Endpoint '{dirname}' is not configured" in str(pyt_e.value), "Expect what"
    assert "Please create" in str(pyt_e.value), "Expect suggested action"


@pytest.mark.parametrize(
    "data", [(True, '{"foo": "bar"}'), (False, '{"foo": "bar", }')]
)
def test_load_user_config_schema(mock_log, data: t.Tuple[bool, str]):
    is_valid, schema_json = data

    conf_dir = pathlib.Path("/")
    template_path = Endpoint.user_config_schema_path(conf_dir)
    template_path.write_text(schema_json)

    if is_valid:
        schema = load_user_config_schema(template_path)
        assert schema == json.loads(schema_json)
    else:
        with pytest.raises(json.JSONDecodeError):
            load_user_config_schema(template_path)
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

    assert load_user_config_template(template_path) == template_str


def test_load_user_config_template_tries_yaml_if_j2_not_found(mock_log):
    conf_dir = pathlib.Path("/")
    template_path_yaml = conf_dir / "user_config_template.yaml"
    template_path_yaml.write_text("yaml")
    template_path_j2 = conf_dir / "user_config_template.yaml.j2"

    assert load_user_config_template(template_path_j2) == "yaml"
    a, *_ = mock_log.info.call_args
    assert "user_config_template.yaml.j2 does not exist" in a[0]


@pytest.mark.parametrize(
    "data",
    [
        (True, {"heartbeat": 10}),
        (True, {"heartbeat": 10, "foo": "bar"}),
        (False, {}),
        (False, {"foo": "bar"}),
    ],
)
def test_render_user_config(
    mock_log, conf_no_exec, data, mapped_ident: MappedPosixIdentity
):
    is_valid, user_opts = data
    conf_dir = pathlib.Path("/")
    template = "heartbeat_period: {{ heartbeat }}"

    if is_valid:
        rendered = render_config_user_template(
            conf_no_exec, template, conf_dir, mapped_ident, {}, user_opts
        )
        rendered_dict = yaml.safe_load(rendered)
        assert rendered_dict["heartbeat_period"] == user_opts["heartbeat"]
    else:
        with pytest.raises(jinja2.exceptions.UndefinedError):
            render_config_user_template(
                conf_no_exec, template, conf_dir, mapped_ident, {}, user_opts
            )
        a, _k = mock_log.debug.call_args
        assert "Missing required" in a[0]
