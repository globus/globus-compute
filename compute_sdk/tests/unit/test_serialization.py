import inspect
import json
import random
import sys
import typing as t
from collections import OrderedDict
from unittest.mock import patch

import dill
import globus_compute_sdk.serialize.concretes as concretes
import globus_compute_sdk.serialize.facade as facade
import pytest
from globus_compute_sdk.errors import (
    DeserializationError,
    SerdeError,
    SerializationError,
)
from globus_compute_sdk.serialize.base import (
    IDENTIFIER_LENGTH,
    ComboSerializationStrategy,
    SerializationStrategy,
)
from globus_compute_sdk.serialize.facade import (
    ComputeSerializer,
    ValidatedStrategylike,
    validate_strategylike,
)
from pytest_mock import MockerFixture

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup


def foo(x, y=3):
    return x * y


def double_first_argument(func):
    """
    Test decorator that will double the first argument if third is True
    """

    def wrapper_double_first_argument(a, b, c):
        return func(a * 2, b, c) if c else func(a, b, c)

    return wrapper_double_first_argument


@double_first_argument
def decorated_add(a, b, c):
    """
    Third argument is ignored, only for decorator usage
    """
    return a + b


@pytest.fixture
def unknown_strategy():
    class UnknownStrategy(SerializationStrategy):
        identifier = "aa\n"
        for_code = True

        def serialize(self, data):
            pass

        def deserialize(self, payload):
            pass

    yield UnknownStrategy

    SerializationStrategy._CACHE.pop(UnknownStrategy.identifier)


def check_deserialized_foo(f: t.Callable):
    p1, p2 = inspect.signature(f).parameters.items()

    param_1: inspect.Parameter = p1[1]
    param_2: inspect.Parameter = p2[1]

    assert isinstance(param_1, inspect.Parameter)
    assert isinstance(param_2, inspect.Parameter)
    assert "x" == param_1.name
    assert "y" == param_2.name
    assert 3 == param_2.default


def check_deserialized_bar(f: t.Callable):
    p1, p2 = inspect.signature(f).parameters.items()

    param_1: inspect.Parameter = p1[1]
    param_2: inspect.Parameter = p2[1]

    assert isinstance(param_1, inspect.Parameter)
    assert isinstance(param_2, inspect.Parameter)
    assert "a" == param_1.name
    assert "b" == param_2.name
    assert 5 == param_2.default


def check_serialize_deserialize_foo(concrete_cls, pre_serialized=None):
    serialized_foo = concrete_cls.serialize(foo)
    deserialized_foo = concrete_cls.deserialize(serialized_foo)
    check_deserialized_foo(deserialized_foo)

    if pre_serialized:
        deserialized_foo = concrete_cls.deserialize(pre_serialized)
        check_deserialized_foo(deserialized_foo)


def check_serialize_deserialize_bar(concrete_cls, pre_serialized=None):
    def inner_bar(a, b=5):
        return a * 5

    serialized_bar = concrete_cls.serialize(inner_bar)
    deserialized_bar = concrete_cls.deserialize(serialized_bar)
    check_deserialized_bar(deserialized_bar)

    if pre_serialized:
        deserialized_bar = concrete_cls.deserialize(pre_serialized)
        check_deserialized_bar(deserialized_bar)


def check_serialize_deserialize_add(concrete_cls, pre_serialized=None):
    serialized_add = concrete_cls.serialize(decorated_add)
    deserialized_add = concrete_cls.deserialize(serialized_add)
    assert 10 == deserialized_add(3, 4, True)
    assert 7 == deserialized_add(3, 4, False)

    if pre_serialized:
        deserialized_add = concrete_cls.deserialize(pre_serialized)
        assert 10 == deserialized_add(3, 4, True)
        assert 7 == deserialized_add(3, 4, False)


def test_base64_data():
    jb = concretes.DillDataBase64()

    d = jb.serialize(([2], {"y": 10}))
    assert "00" == d.split("\n")[0]
    args, kwargs = jb.deserialize(d)
    assert args[0] == 2
    assert kwargs["y"] == 10

    # Depends on running this test in non 3.7 and passing
    py37_serial_data = "00\ngANdcQBLAmF9cQFYAQAAAHlxAksKc4ZxAy4=\n"

    args, kwargs = jb.deserialize(py37_serial_data)
    assert args[0] == 2
    assert kwargs["y"] == 10


def test_json_data():
    strategy = concretes.JSONData()
    data = {"foo": "bar"}
    serialized = strategy.serialize(data)
    deserialized = strategy.deserialize(serialized)
    assert deserialized == data


def test_json_data_handles_legacy_serialization():
    strategy = concretes.JSONData()
    data = {"foo": "bar"}

    # Before SDK version 4.0.0, the JSONData strategy did not encode
    # the data with base64
    legacy_serialized = concretes.JSONData.identifier + json.dumps(data)
    legacy_deserialized = strategy.deserialize(legacy_serialized)

    strategy = concretes.JSONData()
    serialized = strategy.serialize(data)
    deserialized = strategy.deserialize(serialized)

    assert deserialized == legacy_deserialized


def test_pickle_deserialize():
    jb = concretes.PickleCode()

    with pytest.raises(NotImplementedError):
        jb.serialize(foo)

    # Serialized via 3.7, should work in other versions
    serialized_foo_pickle = "02\ngANjdGVzdF9zZXJpYWxpemF0aW9uCmZvbwpxAC4="

    deserialized_foo = jb.deserialize(serialized_foo_pickle)
    check_deserialized_foo(deserialized_foo)


@pytest.mark.skipif(
    not ((3, 7) < sys.version_info < (3, 8) and (3, 9) < sys.version_info < (3, 11)),
    reason=(
        "mismatched python version serialization is suspect, and currently fails"
        " on 3.8 and 3.11"
    ),
)
def test_code_dill():
    """
    This is our default primary path going forward, and it should handle
    the most methods.
    """
    pre_serialized_foo_37 = "01\ngANjdGVzdF9zZXJpYWxpemF0aW9uCmZvbwpxAC4=\n"
    check_serialize_deserialize_foo(concretes.DillCode(), pre_serialized_foo_37)
    pre_serialized_bar_37 = (
        "01\ngANjZGlsbC5fZGlsbApfY3JlYXRlX2Z1bmN0aW9uCnEAKGNkaWxsLl9kaWxsCl9"
        "jcmVhdGVfY29kZQpxAShLAksASwJLAktTQwh8AGQBFABTAHECTksFhnEDKVgBAAAAYX"
        "EEWAEAAABicQWGcQZYRwAAAC9Vc2Vycy9sZWkvZ2xvYi9mdW5jWC9mdW5jeF9zZGsvd"
        "GVzdHMvaW50ZWdyYXRpb24vdGVzdF9zZXJpYWxpemF0aW9uLnB5cQdYCQAAAGlubmVy"
        "X2JhcnEISzhDAgABcQkpKXRxClJxC2N0ZXN0X3NlcmlhbGl6YXRpb24KX19kaWN0X18"
        "KaAhLBYVxDE50cQ1ScQ59cQ99cRAoWA8AAABfX2Fubm90YXRpb25zX19xEX1xElgMAA"
        "AAX19xdWFsbmFtZV9fcRNYMgAAAGNoZWNrX3NlcmlhbGl6ZV9kZXNlcmlhbGl6ZV9iY"
        "XIuPGxvY2Fscz4uaW5uZXJfYmFycRR1hnEVYi4=\n"
    )
    check_serialize_deserialize_bar(
        concretes.DillCode(), pre_serialized=pre_serialized_bar_37
    )
    pre_serialized_add_37 = (
        "01\n"
        "gANjZGlsbC5fZGlsbApfY3JlYXRlX2Z1bmN0aW9uCnEAKGNkaWxsLl9kaWxsCl9jcmVhdGVfY29k\n"
        "ZQpxAShLA0sASwNLBEsTQyB8AnIUiAB8AGQBFAB8AXwCgwNTAIgAfAB8AXwCgwNTAHECTksChnED\n"
        "KVgBAAAAYXEEWAEAAABicQVYAQAAAGNxBodxB1hHAAAAL1VzZXJzL2xlaS9nbG9iL2Z1bmNYL2Z1\n"
        "bmN4X3Nkay90ZXN0cy9pbnRlZ3JhdGlvbi90ZXN0X3NlcmlhbGl6YXRpb24ucHlxCFgdAAAAd3Jh\n"
        "cHBlcl9kb3VibGVfZmlyc3RfYXJndW1lbnRxCUsNQwIAAXEKWAQAAABmdW5jcQuFcQwpdHENUnEO\n"
        "Y3Rlc3Rfc2VyaWFsaXphdGlvbgpfX2RpY3RfXwpoCU5jZGlsbC5fZGlsbApfY3JlYXRlX2NlbGwK\n"
        "cQ9OhXEQUnERhXESdHETUnEUfXEVfXEWKFgPAAAAX19hbm5vdGF0aW9uc19fcRd9cRhYDAAAAF9f\n"
        "cXVhbG5hbWVfX3EZWDwAAABkb3VibGVfZmlyc3RfYXJndW1lbnQuPGxvY2Fscz4ud3JhcHBlcl9k\n"
        "b3VibGVfZmlyc3RfYXJndW1lbnRxGnWGcRtiY2J1aWx0aW5zCmdldGF0dHIKcRxjZGlsbApfZGls\n"
        "bApxHVgIAAAAX3NldGF0dHJxHmNidWlsdGlucwpzZXRhdHRyCnEfh3EgUnEhaBFYDQAAAGNlbGxf\n"
        "Y29udGVudHNxImgAKGgBKEsDSwBLA0sCS0NDCHwAfAEXAFMAcSNYPQAAAAogICAgVGhpcmQgYXJn\n"
        "dW1lbnQgaXMgaWdub3JlZCwgb25seSBmb3IgZGVjb3JhdG9yIHVzYWdlCiAgICBxJIVxJSloBGgF\n"
        "aAaHcSZoCFgNAAAAZGVjb3JhdGVkX2FkZHEnSxJDAgAFcSgpKXRxKVJxKmN0ZXN0X3NlcmlhbGl6\n"
        "YXRpb24KX19kaWN0X18KaCdOTnRxK1JxLH1xLX1xLihYBwAAAF9fZG9jX19xL2gkaBd9cTB1hnEx\n"
        "YodxMlIwLg==\n"
    )
    pre_serialized_add_310 = (
        "01\n"
        "gASVxAIAAAAAAACMCmRpbGwuX2RpbGyUjBBfY3JlYXRlX2Z1bmN0aW9ulJOUKGgAjAxfY3JlYXRl\n"
        "X2NvZGWUk5QoSwNLAEsASwNLBEsTQyB8AnIKiAB8AGQBFAB8AXwCgwNTAIgAfAB8AXwCgwNTAJRO\n"
        "SwKGlCmMAWGUjAFilIwBY5SHlIxHL1VzZXJzL2xlaS9nbG9iL2Z1bmNYL2Z1bmN4X3Nkay90ZXN0\n"
        "cy9pbnRlZ3JhdGlvbi90ZXN0X3NlcmlhbGl6YXRpb24ucHmUjB13cmFwcGVyX2RvdWJsZV9maXJz\n"
        "dF9hcmd1bWVudJRLEkMCAAGUjARmdW5jlIWUKXSUUpRjdGVzdF9zZXJpYWxpemF0aW9uCl9fZGlj\n"
        "dF9fCmgMTmgAjAxfY3JlYXRlX2NlbGyUk5ROhZRSlIWUdJRSlH2UfZQojA9fX2Fubm90YXRpb25z\n"
        "X1+UfZSMDF9fcXVhbG5hbWVfX5SMPGRvdWJsZV9maXJzdF9hcmd1bWVudC48bG9jYWxzPi53cmFw\n"
        "cGVyX2RvdWJsZV9maXJzdF9hcmd1bWVudJR1hpRijAhidWlsdGluc5SMB2dldGF0dHKUk5SMC2Rp\n"
        "bGwuX3NoaW1zlIwFX2RpbGyUk5SMCF9zZXRhdHRylGggjAdzZXRhdHRylJOUh5RSlGgVjA1jZWxs\n"
        "X2NvbnRlbnRzlGgCKGgEKEsDSwBLAEsDSwJLQ0MIfAB8ARcAUwCUjD0KICAgIFRoaXJkIGFyZ3Vt\n"
        "ZW50IGlzIGlnbm9yZWQsIG9ubHkgZm9yIGRlY29yYXRvciB1c2FnZQogICAglIWUKWgKaAuMDWRl\n"
        "Y29yYXRlZF9hZGSUSxhDAgAFlCkpdJRSlGN0ZXN0X3NlcmlhbGl6YXRpb24KX19kaWN0X18KaC9O\n"
        "TnSUUpR9lH2UKIwHX19kb2NfX5RoLWgbfZR1hpRih5RSMC4=\n"
    )

    pre_serialized_add_39 = (
        "01\n"
        "gASVxAIAAAAAAACMCmRpbGwuX2RpbGyUjBBfY3JlYXRlX2Z1bmN0aW9ulJOUKGgAjAxfY3JlYXRl\n"
        "X2NvZGWUk5QoSwNLAEsASwNLBEsTQyB8AnIUiAB8AGQBFAB8AXwCgwNTAIgAfAB8AXwCgwNTAJRO\n"
        "SwKGlCmMAWGUjAFilIwBY5SHlIxHL1VzZXJzL2xlaS9nbG9iL2Z1bmNYL2Z1bmN4X3Nkay90ZXN0\n"
        "cy9pbnRlZ3JhdGlvbi90ZXN0X3NlcmlhbGl6YXRpb24ucHmUjB13cmFwcGVyX2RvdWJsZV9maXJz\n"
        "dF9hcmd1bWVudJRLEkMCAAGUjARmdW5jlIWUKXSUUpRjdGVzdF9zZXJpYWxpemF0aW9uCl9fZGlj\n"
        "dF9fCmgMTmgAjAxfY3JlYXRlX2NlbGyUk5ROhZRSlIWUdJRSlH2UfZQojA9fX2Fubm90YXRpb25z\n"
        "X1+UfZSMDF9fcXVhbG5hbWVfX5SMPGRvdWJsZV9maXJzdF9hcmd1bWVudC48bG9jYWxzPi53cmFw\n"
        "cGVyX2RvdWJsZV9maXJzdF9hcmd1bWVudJR1hpRijAhidWlsdGluc5SMB2dldGF0dHKUk5SMC2Rp\n"
        "bGwuX3NoaW1zlIwFX2RpbGyUk5SMCF9zZXRhdHRylGggjAdzZXRhdHRylJOUh5RSlGgVjA1jZWxs\n"
        "X2NvbnRlbnRzlGgCKGgEKEsDSwBLAEsDSwJLQ0MIfAB8ARcAUwCUjD0KICAgIFRoaXJkIGFyZ3Vt\n"
        "ZW50IGlzIGlnbm9yZWQsIG9ubHkgZm9yIGRlY29yYXRvciB1c2FnZQogICAglIWUKWgKaAuMDWRl\n"
        "Y29yYXRlZF9hZGSUSxhDAgAFlCkpdJRSlGN0ZXN0X3NlcmlhbGl6YXRpb24KX19kaWN0X18KaC9O\n"
        "TnSUUpR9lH2UKIwHX19kb2NfX5RoLWgbfZR1hpRih5RSMC4=\n"
    )

    pre_serialized = pre_serialized_add_37
    mismatch_serialized = pre_serialized_add_39

    # dill.dumps()/loads() does not deserialize functions serialized with some
    #     other python versions.  Some info:
    #     https://github.com/cloudpipe/cloudpickle/issues/293
    # TODO figure out a better way to deserialize mismatched versions
    #     or possibly give up on trying to deserialize mismatched versions
    # For now, just test pre-generated strings for 3.7, 3.9, and 3.10, and
    #     force version upgrades to fail in tox until a known string is
    #     generated in check_serialize_deserialize_add() above
    major = sys.version_info.major
    minor = sys.version_info.minor
    if major == 3 and minor == 9:
        pre_serialized = pre_serialized_add_39
        mismatch_serialized = pre_serialized_add_37
    elif major == 3 and minor == 10:
        pre_serialized = pre_serialized_add_310
    elif major == 3 and minor > 10:
        # Failing probably better than forgetting about version mismatches
        # in the future
        pytest.fail(f"Pre-serialized string needed for py{major}.{minor}")

    check_serialize_deserialize_add(concretes.DillCode(), pre_serialized)

    # dill code doesn't handle mismatched py versions
    deserialized_add = concretes.DillCode().deserialize(mismatch_serialized)
    assert 10 == deserialized_add(3, 4, True)


def test_code_text_inspect():
    check_serialize_deserialize_foo(concretes.DillCodeTextInspect())


def test_code_dill_source():
    check_serialize_deserialize_foo(concretes.DillCodeSource())

    # dill code source doesn't handle the indents of bar
    check_serialize_deserialize_bar(concretes.DillCodeSource())


def test_pure_source_text_inspect():
    check_serialize_deserialize_foo(concretes.PureSourceTextInspect())


def test_pure_source_dill():
    check_serialize_deserialize_foo(concretes.PureSourceDill())
    check_serialize_deserialize_bar(concretes.PureSourceDill())


def test_pure_source_text_inspect_handles_legacy_serialization():
    # Before SDK version 3.16.0, the PureSourceTextInspect strategy did not
    # encode the function source code with base64
    def legacy_serialize(f: t.Callable):
        name: str = f.__name__
        body = inspect.getsource(f)
        return "st\n" + name + ":" + body

    check_serialize_deserialize_foo(
        concretes.PureSourceTextInspect(), pre_serialized=legacy_serialize(foo)
    )


def test_pure_source_dill_handles_legacy_serialization():
    # Before SDK version 3.16.0, the PureSourceDill strategy did not encode
    # the function source code with base64
    def legacy_serialize(f: t.Callable):
        name: str = f.__name__
        body = dill.source.getsource(f, lstrip=True)
        return "sd\n" + name + ":" + body

    check_serialize_deserialize_foo(
        concretes.PureSourceDill(), pre_serialized=legacy_serialize(foo)
    )


def test_overall():
    check_serialize_deserialize_foo(ComputeSerializer())
    check_serialize_deserialize_bar(ComputeSerializer())


@pytest.mark.parametrize(
    "strategy_cls, data",
    [(concretes.AllCodeStrategies, foo), (concretes.AllDataStrategies, "data")],
)
def test_combo_strategy_serialize_fail(
    strategy_cls: t.Type[ComboSerializationStrategy], data: t.Any, mocker: MockerFixture
):
    strategy = strategy_cls()
    for s in strategy.strategies:
        mocker.patch.object(s, "serialize", side_effect=Exception("Bad boy!"))

    with pytest.raises(SerializationError) as e_info:
        strategy.serialize(data)

    assert "Serialization failed" in str(e_info.value)
    for s in strategy.strategies:
        assert s.__name__ in str(e_info.value)


@pytest.mark.parametrize(
    "strategy_cls, data",
    [(concretes.AllCodeStrategies, foo), (concretes.AllDataStrategies, "data")],
)
def test_combo_strategy_serialize_log_failures(
    strategy_cls: t.Type[ComboSerializationStrategy], data: t.Any, mocker: MockerFixture
):
    strategy = strategy_cls()
    sub_strategy_cls = strategy.strategies[
        random.randint(0, len(strategy.strategies) - 1)
    ]
    mocker.patch.object(
        sub_strategy_cls, "serialize", side_effect=Exception("Bad boy!")
    )
    mock_log = mocker.patch("globus_compute_sdk.serialize.base.logger")

    # This should succeed
    strategy.serialize(data)

    a, _ = mock_log.debug.call_args
    assert f"Failed to serialize with {sub_strategy_cls.__name__}" in a[0]


@pytest.mark.parametrize(
    "strategy_cls, data",
    [(concretes.AllCodeStrategies, foo), (concretes.AllDataStrategies, "data")],
)
def test_combo_strategy_deserialize_fail(
    strategy_cls: t.Type[ComboSerializationStrategy], data: t.Any, mocker: MockerFixture
):
    strategy = strategy_cls()
    for s in strategy.strategies:
        mocker.patch.object(s, "deserialize", side_effect=Exception("Bad boy!"))

    serialized = strategy.serialize(data)
    with pytest.raises(DeserializationError) as e_info:
        strategy.deserialize(serialized)

    assert "Deserialization failed" in str(e_info.value)
    for s in strategy.strategies:
        assert s.__name__ in str(e_info.value)


@pytest.mark.parametrize(
    "strategy_cls, data",
    [(concretes.AllCodeStrategies, foo), (concretes.AllDataStrategies, "data")],
)
def test_combo_strategy_deserialize_log_failures(
    strategy_cls: t.Type[ComboSerializationStrategy], data: t.Any, mocker: MockerFixture
):
    strategy = strategy_cls()
    sub_strategy_cls = strategy.strategies[0]
    mocker.patch.object(
        sub_strategy_cls, "deserialize", side_effect=Exception("Bad boy!")
    )
    mock_log = mocker.patch("globus_compute_sdk.serialize.base.logger")

    # This should succeed
    serialized = strategy.serialize(data)
    strategy.deserialize(serialized)

    a, _ = mock_log.debug.call_args
    assert f"Failed to deserialize with {sub_strategy_cls.__name__}" in a[0]


@pytest.mark.parametrize(
    "strategy_cls, data",
    [(concretes.AllCodeStrategies, foo), (concretes.AllDataStrategies, "data")],
)
def test_combo_strategy_deserialize_invalid_strategy(
    strategy_cls: t.Type[ComboSerializationStrategy], data: t.Any, mocker: MockerFixture
):
    strategy = strategy_cls()
    invalid_id = "BAD\n"
    for s in strategy.strategies:
        mocker.patch.object(s, "serialize", return_value=invalid_id)

    serialized = strategy.serialize(data)
    with pytest.raises(DeserializationError) as e_info:
        strategy.deserialize(serialized)

    assert "Invalid strategy identifier" in str(e_info.value)
    assert invalid_id.strip() in str(e_info.value)


@pytest.mark.parametrize(
    "strategy_cls, data",
    [(concretes.AllCodeStrategies, foo), (concretes.AllDataStrategies, "data")],
)
def test_combo_strategy_deserialize_malformed_identifier(
    strategy_cls: t.Type[ComboSerializationStrategy], data: t.Any, mocker: MockerFixture
):
    strategy = strategy_cls()
    malformed_id = "NO_NEWLINE"
    data = f"{malformed_id}{'a' * 100}"
    for s in strategy.strategies:
        mocker.patch.object(s, "serialize", return_value=data)

    serialized = strategy.serialize(foo)
    with pytest.raises(DeserializationError) as e_info:
        strategy.deserialize(serialized)

    assert "Malformed data (no newline separator)" in str(e_info.value)
    assert data[:50] in str(e_info.value)


def test_all_code_strategies():
    all_code = concretes.AllCodeStrategies()
    serialized = all_code.serialize(foo)

    chomped = all_code.chomp(serialized)
    chunks = chomped.split(all_code._separator)
    assert len(chunks) == len(all_code.strategies)

    func = all_code.deserialize(serialized)

    assert callable(func)
    n1, n2 = random.randint(1, 100), random.randint(1, 100)
    assert func(n1, n2) == foo(n1, n2)


@pytest.mark.parametrize("strategy", list(concretes.AllCodeStrategies.strategies))
def test_all_code_strategies_individually(
    strategy: SerializationStrategy, mocker: MockerFixture
):
    # Ensure we isolate each sub-strategy
    # Otherwise, the test will pass if any sub-strategy works
    mocker.patch.object(
        concretes.AllCodeStrategies,
        "strategies",
        [strategy],
    )

    all_code = concretes.AllCodeStrategies()
    serialized = all_code.serialize(foo)
    func = all_code.deserialize(serialized)

    assert callable(func)
    n1, n2 = random.randint(1, 100), random.randint(1, 100)
    assert func(n1, n2) == foo(n1, n2)


def test_all_data_strategies():
    d1 = "data"
    all_data = concretes.AllDataStrategies()
    serialized = all_data.serialize(d1)

    chomped = all_data.chomp(serialized)
    chunks = chomped.split(all_data._separator)
    assert len(chunks) == len(all_data.strategies)

    d2 = all_data.deserialize(serialized)

    assert d1 == d2


@pytest.mark.parametrize("strategy", list(concretes.AllDataStrategies.strategies))
def test_all_data_strategies_individually(
    strategy: SerializationStrategy, mocker: MockerFixture
):
    # Ensure we isolate each sub-strategy
    # Otherwise, the test will pass if any sub-strategy works
    mocker.patch.object(
        concretes.AllDataStrategies,
        "strategies",
        [strategy],
    )

    d1 = "data"
    all_data = concretes.AllDataStrategies()
    serialized = all_data.serialize(d1)
    d2 = all_data.deserialize(serialized)

    assert d1 == d2


@pytest.mark.parametrize(
    "strategy", list(concretes.CombinedCode._chunk_strategies.values())
)
def test_combined_strategies(strategy: SerializationStrategy, mocker: MockerFixture):
    # Ensure we isolate each sub-strategy
    # Otherwise, the test will pass if any sub-strategy works
    mocker.patch.object(
        concretes.CombinedCode,
        "_chunk_strategies",
        OrderedDict([(strategy.identifier, strategy)]),
    )

    combined = concretes.CombinedCode()
    serialized = combined.serialize(foo)
    func = combined.deserialize(serialized)

    assert callable(func)
    n1, n2 = random.randint(1, 100), random.randint(1, 100)
    assert func(n1, n2) == foo(n1, n2)


def test_combined_serialize_fail():
    single_source = concretes.DillCodeSource()

    # built-in functions do not serialize with most strategies
    with pytest.raises(TypeError):
        single_source.serialize(max)

    combined = concretes.CombinedCode()
    combined_serialized_func = combined.serialize(max)
    deserialized = combined.deserialize(combined_serialized_func)

    assert deserialized(1, 4) == 4


@patch(
    "globus_compute_sdk.serialize.concretes.DillCode.serialize",
    side_effect=Exception("block this"),
)
def test_combined_serialize_all_fail(code_mock):
    # max() only works with DillCode.  Disabling that should trigger
    # a failure when no methods work
    combined = concretes.CombinedCode()
    with pytest.raises(SerializationError) as exc_info:
        combined.serialize(max)
    assert "No serialization methods were successful" in str(exc_info.value)


def test_serialize_deserialize_combined():
    f = decorated_add
    combined = concretes.CombinedCode()
    single_source = concretes.DillCodeSource()
    single_code = concretes.DillCode()
    combined_serialized_func = combined.serialize(f)
    source_serialized_func = single_source.serialize(f)
    code_serialized_func = single_code.serialize(f)

    # Deserializing decorated functions does not raise exception,
    # only when one attempts to run it
    # TODO find example that fails on Category 2 (deserial) for DillCodeSource
    # the new default
    # with pytest.raises(Exception) as e:
    deserial_source = single_source.deserialize(source_serialized_func)

    # Only when attempting to run it
    with pytest.raises(NameError):
        assert 11 == deserial_source(3, 5, 2)
        # pass

    code_deserial = single_code.deserialize(code_serialized_func)
    assert 14 == code_deserial(4, 6, 3)

    # assumes DillCodeSource is first strategy that CombinedSerializer tries
    deserialized = combined.deserialize(combined_serialized_func)
    with pytest.raises(NameError):
        _ = deserialized(3, 5, 2)

    alternate_deserialized = combined.deserialize(combined_serialized_func, variation=2)
    assert alternate_deserialized != deserialized


def test_compute_serializer_defaults():
    serializer = ComputeSerializer()

    assert (
        serializer.serialize("something non-callable")[:IDENTIFIER_LENGTH]
        == concretes.DEFAULT_STRATEGY_DATA.identifier
    )

    assert (
        serializer.serialize(foo)[:IDENTIFIER_LENGTH]
        == concretes.DEFAULT_STRATEGY_CODE.identifier
    )


@pytest.mark.parametrize("strategy", concretes.SELECTABLE_STRATEGIES)
def test_selectable_serialization(strategy):
    if strategy.for_code:
        serializer = ComputeSerializer(strategy_code=strategy())
        data = foo
    else:
        serializer = ComputeSerializer(strategy_data=strategy())
        data = "foo"
    ser_data = serializer.serialize(data)
    assert ser_data[:IDENTIFIER_LENGTH] == strategy.identifier


def test_selectable_serialization_validates_strategies(mocker):
    mock_validate_strategylike = mocker.patch(
        "globus_compute_sdk.serialize.facade.validate_strategylike"
    )

    strategy_code = concretes.DillCode()
    strategy_data = concretes.JSONData()
    ComputeSerializer(strategy_code, strategy_data)

    assert mock_validate_strategylike.call_count == 2
    mock_validate_strategylike.assert_any_call(strategy_code, for_code=True)
    mock_validate_strategylike.assert_any_call(strategy_data, for_code=False)


_validated_dill_code = ValidatedStrategylike(
    concretes.DillCode,
    SerializationStrategy.get_cached_by_class(concretes.DillCode),
    "globus_compute_sdk.serialize.concretes.DillCode",
)

_validated_json_data = ValidatedStrategylike(
    concretes.JSONData,
    SerializationStrategy.get_cached_by_class(concretes.JSONData),
    "globus_compute_sdk.serialize.concretes.JSONData",
)


@pytest.mark.parametrize(
    "strategylike, validated",
    [
        (concretes.DillCode, _validated_dill_code),
        ("globus_compute_sdk.serialize.concretes.DillCode", _validated_dill_code),
        (concretes.DillCode(), _validated_dill_code),
        (concretes.JSONData, _validated_json_data),
        ("globus_compute_sdk.serialize.concretes.JSONData", _validated_json_data),
        (concretes.JSONData(), _validated_json_data),
    ],
)
def test_validate_strategylike(strategylike, validated):
    assert validate_strategylike(strategylike) == validated


@pytest.mark.parametrize("strategy", concretes.SELECTABLE_STRATEGIES)
def test_validate_strategylike_enforces_for_code(strategy):
    with pytest.raises(SerdeError) as pyt_exc:
        validate_strategylike(strategy(), for_code=not strategy.for_code)

    if strategy.for_code:
        e = "is a code serialization strategy, expected a data strategy"
    else:
        e = "is a data serialization strategy, expected a code strategy"
    assert e in str(pyt_exc)


def test_validate_strategylike_errors_on_unknown_strategy(unknown_strategy):
    with pytest.raises(SerdeError) as pyt_exc:
        validate_strategylike(unknown_strategy())

    assert "is not a known serialization strategy" in str(pyt_exc)


@pytest.mark.parametrize(
    "bad_path",
    [
        "invalid_path_1",
        "invalid path 2",
        "",
    ],
)
def test_validate_strategylike_errors_on_bad_import_path(bad_path):
    with pytest.raises(SerdeError) as pyt_exc:
        validate_strategylike(bad_path)

    assert "an import path" in str(pyt_exc)


def test_validate_strategylike_enforces_subclass():
    class NotAStrategy:
        pass

    with pytest.raises(SerdeError) as pyt_exc:
        validate_strategylike(NotAStrategy())

    assert "a subclass of" in str(pyt_exc)


@pytest.mark.parametrize(
    "strategy_code", (s for s in concretes.SELECTABLE_STRATEGIES if s.for_code)
)
@pytest.mark.parametrize(
    "strategy_data", (s for s in concretes.SELECTABLE_STRATEGIES if not s.for_code)
)
@pytest.mark.parametrize(
    "function, args, kwargs",
    [(foo, (random.random(),), {}), (foo, (random.random(),), {"y": random.random()})],
)
def test_check_strategies(strategy_code, strategy_data, function, args, kwargs):
    serializer = ComputeSerializer(
        strategy_code=strategy_code(), strategy_data=strategy_data()
    )

    new_fn, new_args, new_kwargs = serializer.check_strategies(
        function, *args, **kwargs
    )

    original_result = function(*args, **kwargs)
    new_result = new_fn(*new_args, **new_kwargs)

    assert original_result == new_result


@pytest.mark.parametrize("disallowed_strategy", concretes.SELECTABLE_STRATEGIES)
def test_allowed_deserializers(disallowed_strategy):
    allowlist = [
        strategy
        for strategy in concretes.SELECTABLE_STRATEGIES
        if strategy != disallowed_strategy
    ]

    assert allowlist, "expect to have at least one allowed deserializer"

    serializer = ComputeSerializer(allowed_deserializer_types=allowlist)
    payload = disallowed_strategy().serialize(
        foo if disallowed_strategy.for_code else "foo"
    )

    with pytest.raises(DeserializationError) as pyt_exc:
        serializer.deserialize(payload)
    assert f"serializer {disallowed_strategy.__name__} disabled" in str(pyt_exc)


@pytest.mark.parametrize(
    "allowlist",
    [
        [
            "globus_compute_sdk.serialize.concretes.DillCode",
            "globus_compute_sdk.serialize.JSONData",
        ],
        [f"{s.__module__}.{s.__qualname__}" for s in concretes.SELECTABLE_STRATEGIES],
    ],
)
def test_allowed_deserializers_imports_from_path(allowlist):
    serializer = ComputeSerializer(allowed_deserializer_types=allowlist)
    assert len(serializer.allowed_deserializer_types) == len(allowlist)


@pytest.mark.parametrize(
    "allowlist",
    [
        ["my_malicious_package.my_malicious_serializer"],
        ["invalid_path_1"],
        ["invalid path 2"],
        [""],
        [
            "globus_compute_sdk.serialize.concretes.DillCode",
            "my_malicious_package.my_malicious_serializer",
        ],
        [
            "globus_compute_sdk.serialize.concretes.DillCode",
            "invalid path",
        ],
        [
            "globus_compute_sdk.serialize.concretes.DillCode",
            "",
        ],
    ],
)
def test_allowed_deserializers_errors_on_invalid_import_path(allowlist):
    with pytest.raises(SerdeError) as pyt_exc:
        ComputeSerializer(allowed_deserializer_types=allowlist)
    assert "an import path" in str(pyt_exc)


@pytest.mark.parametrize(
    "allowlist",
    [
        [facade.AllowlistWildcard.CODE, facade.AllowlistWildcard.DATA],
        [facade.AllowlistWildcard.CODE, concretes.JSONData],
        [facade.AllowlistWildcard.CODE, "globus_compute_sdk.serialize.DillDataBase64"],
        (
            [facade.AllowlistWildcard.CODE]
            + [s for s in concretes.SELECTABLE_STRATEGIES if not s.for_code]
        ),
        [facade.AllowlistWildcard.DATA, concretes.CombinedCode],
        [facade.AllowlistWildcard.DATA, "globus_compute_sdk.serialize.DillCodeSource"],
        (
            [facade.AllowlistWildcard.DATA]
            + [s for s in concretes.SELECTABLE_STRATEGIES if s.for_code]
        ),
    ],
)
def test_allowed_deserializers_wildcards(allowlist):
    serializer = ComputeSerializer(allowed_deserializer_types=allowlist)
    parsedlist = serializer.allowed_deserializer_types

    if facade.AllowlistWildcard.CODE in allowlist:
        assert any(s.for_code for s in parsedlist)
        assert all(
            s in parsedlist for s in concretes.SELECTABLE_STRATEGIES if s.for_code
        )

    if facade.AllowlistWildcard.DATA in allowlist:
        assert any(not s.for_code for s in parsedlist)
        assert all(
            s in parsedlist for s in concretes.SELECTABLE_STRATEGIES if not s.for_code
        )


@pytest.mark.parametrize(
    "codeWildcard",
    [
        facade.AllowlistWildcard.CODE,
        facade.AllowlistWildcard.CODE.value,
        "globus_compute_sdk.*Code",
    ],
)
@pytest.mark.parametrize(
    "dataWildcard",
    [
        facade.AllowlistWildcard.DATA,
        facade.AllowlistWildcard.DATA.value,
        "globus_compute_sdk.*Data",
    ],
)
def test_allowed_deserializers_wildcards_instance_or_value(codeWildcard, dataWildcard):
    serializer = ComputeSerializer(
        allowed_deserializer_types=[codeWildcard, dataWildcard]
    )
    parsedlist = serializer.allowed_deserializer_types

    assert all(s in parsedlist for s in concretes.SELECTABLE_STRATEGIES if s.for_code)
    assert all(
        s in parsedlist for s in concretes.SELECTABLE_STRATEGIES if not s.for_code
    )


@pytest.mark.parametrize(
    "allowlist",
    [
        *[
            [facade.AllowlistWildcard.CODE, s]
            for s in concretes.SELECTABLE_STRATEGIES
            if s.for_code
        ],
        *[
            [facade.AllowlistWildcard.DATA, s]
            for s in concretes.SELECTABLE_STRATEGIES
            if not s.for_code
        ],
    ],
)
def test_allowed_deserializers_wildcards_errors_on_duplicate(allowlist):
    with pytest.raises(SerdeError) as pyt_exc:
        ComputeSerializer(allowed_deserializer_types=allowlist)

    expected = f"Cannot mix '{allowlist[0]}' with specific deserializers"
    assert expected in str(pyt_exc)


@pytest.mark.parametrize(
    "allowlist",
    (
        [s for s in concretes.SELECTABLE_STRATEGIES if s.for_code],
        [s for s in concretes.SELECTABLE_STRATEGIES if not s.for_code],
        [
            f"{s.__module__}.{s.__qualname__}"
            for s in concretes.SELECTABLE_STRATEGIES
            if s.for_code
        ],
        [
            f"{s.__module__}.{s.__qualname__}"
            for s in concretes.SELECTABLE_STRATEGIES
            if not s.for_code
        ],
        [facade.AllowlistWildcard.CODE],
        [facade.AllowlistWildcard.DATA],
    ),
)
def test_allowed_deserializers_enforces_both_code_and_data(allowlist):
    with pytest.raises(SerdeError) as pyt_exc:
        ComputeSerializer(allowed_deserializer_types=allowlist)

    assert "at least one code and one data" in str(pyt_exc)


@pytest.mark.parametrize(
    "non_subclass",
    [
        type(
            "NonSubclassedStrategy",
            (),
            {
                "identifier": "aa\n",
                "for_code": True,
                "serialize": lambda self, data: None,
                "deserialize": lambda self, payload: None,
            },
        ),
        int,
        {"foo": "bar"},
    ],
)
def test_allowed_deserializers_enforces_subclass(non_subclass):
    with pytest.raises(SerdeError) as pyt_exc:
        ComputeSerializer(allowed_deserializer_types=[non_subclass])

    assert "Invalid strategy-like" in str(pyt_exc)


def test_allowed_deserializers_errors_on_unknown_strategy(unknown_strategy):
    with pytest.raises(SerdeError) as pyt_exc1:
        ComputeSerializer(allowed_deserializer_types=[unknown_strategy])

    unknown_strategy.for_code = False

    with pytest.raises(SerdeError) as pyt_exc2:
        ComputeSerializer(allowed_deserializer_types=[unknown_strategy])

    for e in (pyt_exc1, pyt_exc2):
        assert "is not a known serialization strategy" in str(e)


@pytest.mark.parametrize("wrong_length_id", ["", "1", "12", "1234"])
def test_init_subclass_requires_correct_length(wrong_length_id):
    with pytest.raises(ValueError) as pyt_exc:

        class NewStrategy(SerializationStrategy):
            identifier = wrong_length_id
            for_code = True

            def serialize(self, data):
                pass

            def deserialize(self, payload):
                pass

    assert "must be 3 characters long" in str(pyt_exc.value)


def test_init_subclass_requires_newline():
    with pytest.raises(ValueError) as pyt_exc:

        class NewStrategy(SerializationStrategy):
            identifier = "123"
            for_code = True

            def serialize(self, data):
                pass

            def deserialize(self, payload):
                pass

    assert "must end with a newline character" in str(pyt_exc.value)


@pytest.mark.parametrize(
    "existing_id", [s.identifier for s in SerializationStrategy._CACHE.values()]
)
def test_init_subclass_requires_unique_identifier(existing_id):
    with pytest.raises(ValueError) as pyt_exc:

        class NewStrategy(SerializationStrategy):
            identifier = existing_id
            for_code = True

            def serialize(self, data):
                pass

            def deserialize(self, payload):
                pass

    assert f"{existing_id!r} is already used by" in str(pyt_exc.value)


@pytest.mark.parametrize(
    "data, strategies",
    [
        (foo, [concretes.DillCode(), concretes.DillCodeSource()]),
        (foo, ["gibberish", concretes.DillCode()]),
        (foo, [concretes.DillDataBase64(), concretes.PureSourceDill()]),
        (foo, [concretes.JSONData(), concretes.CombinedCode()]),
        (foo, concretes.SELECTABLE_STRATEGIES),
        ("foo", [concretes.JSONData()]),
        ("foo", ["gibberish", concretes.JSONData()]),
        ("foo", [concretes.PureSourceTextInspect(), concretes.DillDataBase64()]),
        ("foo", [concretes.DillDataBase64(), concretes.JSONData()]),
        ("foo", concretes.SELECTABLE_STRATEGIES),
    ],
)
def test_serialize_from_list_happy_path(data, strategies):
    ComputeSerializer.serialize_from_list(data, strategies)


@pytest.mark.parametrize("data", [foo, "foo"])
def test_serialize_from_list_uses_default_strategies_when_empty(data):
    buffer = ComputeSerializer.serialize_from_list(data, [])
    buf_id = buffer[:IDENTIFIER_LENGTH]
    expected_id = (
        concretes.DEFAULT_STRATEGY_CODE.identifier
        if callable(data)
        else concretes.DEFAULT_STRATEGY_DATA.identifier
    )
    assert buf_id == expected_id


@pytest.mark.parametrize(
    "strategies",
    [
        ["gibberish", "strings", "alkjfdhaf"],
        ["not", "strategylike", "objects", None, 123, object()],
        [
            "bad",
            "import",
            "paths",
            "globus_compute_sdk.*",
            "globus_compute_sdk JSONData",
        ],
    ],
)
def test_serialize_from_list_rejects_invalid_strategylikes(strategies):
    with pytest.raises(SerdeError) as pyt_exc:
        ComputeSerializer.serialize_from_list(foo, strategies)

    excgroup = pyt_exc.value.__cause__
    assert isinstance(excgroup, ExceptionGroup)
    assert len(excgroup.exceptions) == len(strategies)
    assert all("Invalid strategy-like" in str(e) for e in excgroup.exceptions)


def test_serialize_from_list_rejects_unknown_strategy(unknown_strategy):
    with pytest.raises(SerdeError) as pyt_exc:
        ComputeSerializer.serialize_from_list(foo, [unknown_strategy])

    excgroup = pyt_exc.value.__cause__
    assert isinstance(excgroup, ExceptionGroup)
    assert len(excgroup.exceptions) == 1
    assert unknown_strategy.__name__ in str(excgroup.exceptions[0])
    assert "not a known serialization strategy" in str(excgroup.exceptions[0])


@pytest.mark.parametrize(
    "data, strategies",
    [
        (foo, [s for s in concretes.SELECTABLE_STRATEGIES if not s.for_code]),
        ("foo", [s for s in concretes.SELECTABLE_STRATEGIES if s.for_code]),
    ],
)
def test_serialize_from_list_enforces_for_code(data, strategies):
    with pytest.raises(SerdeError) as pyt_exc:
        ComputeSerializer.serialize_from_list(data, strategies)

    excgroup = pyt_exc.value.__cause__
    assert isinstance(excgroup, ExceptionGroup)
    assert len(excgroup.exceptions) == len(strategies)

    if callable(data):
        exp_msg = "is a data serialization strategy, expected a code strategy"
    else:
        exp_msg = "is a code serialization strategy, expected a data strategy"

    assert all(exp_msg in str(e) for e in excgroup.exceptions)
