import json
import pathlib
import random
import threading
import uuid

import pytest
from globus_compute_endpoint.endpoint.identity_mapper import PosixIdentityMapper
from globus_identity_mapping.mappers import ExternalIdentityMapping
from tests.utils import try_assert

_MOCK_BASE = "globus_compute_endpoint.endpoint.identity_mapper."


def _expression_identity_mapper_conf() -> str:
    return """[{
        "DATA_TYPE": "expression_identity_mapping#1.0.0",
        "mappings": [
            {"source": "{uname}", "match": "(.*)@a.local", "output": "{0}"},
            {"source": "{uname}", "match": "(.*)@b.local", "output": "{0}"}
        ]
    }]"""


def _external_identity_mapper_conf() -> str:
    return """
    [{
        "DATA_TYPE": "external_identity_mapping#1.0.0",
        "command": ["/some/executable.py_or_sh_or_rb_or_exe_or..."]
    }]""".strip()


@pytest.fixture
def conf_p(tmp_path):
    yield tmp_path / "some_file"


class NoWaitPosixIdentityMapper(PosixIdentityMapper):
    def __init__(self, *args, poll_interval_s: float = 0.0, **kwargs):
        self.test_poll_interval_s = poll_interval_s
        super().__init__(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_watching()

    @property
    def poll_interval_s(self) -> float:
        return self.test_poll_interval_s

    @poll_interval_s.setter
    def poll_interval_s(self, new_interval_s: float) -> None:
        pass


def test_fails_to_handle_configuration_load_failures_at_initialization(fs):
    # important to give main thread chance to handle failure (e.g., to quit)
    conf_p = pathlib.Path("/some_file")

    with pytest.raises(FileNotFoundError):
        PosixIdentityMapper(conf_p, "some_id")

    conf_p.touch(mode=0o000)
    with pytest.raises(PermissionError):
        PosixIdentityMapper(conf_p, "some_id")

    conf_p.chmod(mode=0o644)
    conf_p.write_text('{"unclosed": "quote}')
    with pytest.raises(json.JSONDecodeError):
        PosixIdentityMapper(conf_p, "some_id")


def test_poll_interval_setter_enforces_minimum(fs):
    conf_p = pathlib.Path("/some_file")
    conf_p.write_text("[]")
    pim = PosixIdentityMapper(conf_p, "some_id", poll_interval_s=-5)
    assert pim.poll_interval_s >= 0.5

    pim.poll_interval_s = 0.3
    assert pim.poll_interval_s >= 0.5
    pim.stop_watching()


def test_atomically_loads_new_configurations(mocker, conf_p):
    def write_configs():
        assert pim.identity_mappings == []

        mock_log.reset_mock()
        conf_p.write_text("[adsfasdf")
        yield False

        assert pim.identity_mappings == [], "bad config; expect not changed"

        info_a = mock_log.info.call_args[0][0]
        err_a = mock_log.error.call_args[0][0]
        mock_log.reset_mock()
        assert "change detected" in info_a
        assert "rereading" in info_a
        assert str(conf_p) in info_a
        assert "Unable to update identity configuration" in err_a

        conf_p.write_text(_external_identity_mapper_conf())
        yield False

        assert isinstance(pim.identity_mappings, list)
        assert "py_or_sh_or_rb_or_exe_or" in str(pim.identity_mappings)

        info_a = mock_log.info.call_args[0][0]
        mock_log.reset_mock()

        assert "change detected" in info_a
        assert "rereading" in info_a
        assert str(conf_p) in info_a

        conf_p.write_text("]")
        yield False

        assert isinstance(pim.identity_mappings, list)
        assert "py_or_sh_or_rb_or_exe_or" in str(
            pim.identity_mappings
        ), "bad config; expect not changed"

        conf_p.write_text("[]")
        yield False
        assert pim.identity_mappings == []

        yield True

    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mock_evt = mocker.Mock(spec=threading.Event)
    mock_evt.wait.side_effect = write_configs()
    mocker.patch(f"{_MOCK_BASE}threading.Event", return_value=mock_evt)
    mocker.patch(f"{_MOCK_BASE}threading.Thread")

    conf_p.write_text("[]")

    pim = PosixIdentityMapper(conf_p, "some_id")
    pim._poll_config()
    pim.stop_watching()


def test_automatically_loads_new_configurations(mocker, conf_p):
    conf_p.write_text("[]")
    mocker.patch(f"{_MOCK_BASE}log")
    with NoWaitPosixIdentityMapper(conf_p, "some_id") as pim:
        assert pim.identity_mappings == []

        conf_p.write_text(_external_identity_mapper_conf())
        try_assert(lambda: len(pim.identity_mappings) > 0)
        assert "/some/executable.py_or_s" in str(pim.identity_mappings)

        conf_p.write_text("[]")
        try_assert(lambda: len(pim.identity_mappings) == 0)

        conf_p.write_text(_expression_identity_mapper_conf())
        try_assert(lambda: len(pim.identity_mappings) > 0)
        assert "@a.local" in str(pim.identity_mappings)


def test_delete_identity_mappings(conf_p):
    conf_p.write_text(_expression_identity_mapper_conf())
    with NoWaitPosixIdentityMapper(conf_p, "some_id") as pim:
        assert len(pim.identity_mappings) > 0
        del pim.identity_mappings
        try_assert(lambda: pim.identity_mappings == [])


@pytest.mark.parametrize("idset", ((), ({"some": "id"}, {"other": "id"})))
def test_map_identity_falls_back_to_none(conf_p, idset):
    conf_p.write_text(_expression_identity_mapper_conf())
    with NoWaitPosixIdentityMapper(conf_p, "some_id", poll_interval_s=0.001) as pim:
        assert pim.map_identity(idset) is None


@pytest.mark.parametrize(
    "expected,idset",
    (
        ("a", ({"uname": "a@a.local"}, {"uname": "b@b.local"})),
        ("b", ({"uname": "b@b.local"}, {"uname": "a@a.local"})),
        ("b", ({"uname": "a@c.local"}, {"uname": "b@b.local"})),
    ),
)
def test_map_identity_returns_first_found_identity(conf_p, expected, idset):
    conf_p.write_text(_expression_identity_mapper_conf())
    with NoWaitPosixIdentityMapper(conf_p, "some_id", poll_interval_s=0.001) as pim:
        assert pim.map_identity(idset) == expected


def test_map_identity_error_logs_show_type_and_sub(mocker, conf_p):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    idset = tuple({"sub": str(uuid.uuid4())} for _ in range(random.randint(1, 10)))

    ext_conf = json.loads(_external_identity_mapper_conf())
    ext_conf.extend(ext_conf[0] for _ in range(random.randint(0, 5)))
    conf_p.write_text(json.dumps(ext_conf))
    with NoWaitPosixIdentityMapper(conf_p, "some_id", poll_interval_s=0.001) as pim:
        pim.map_identity(idset)

    num_idents = len(idset)
    num_mappers = len(ext_conf)
    mapper_name = ExternalIdentityMapping.__name__
    log_iter = iter(mock_log.warning.call_args_list)
    for m_i, _conf in enumerate(ext_conf, start=1):
        for id_i, ident in enumerate(idset, start=1):
            a, _ = next(log_iter)
            assert f"failed for mapper {m_i} [of {num_mappers}] ({mapper_name}" in a[0]
            assert f"with identity {id_i} [of {num_idents}] ({ident['sub']})" in a[0]
