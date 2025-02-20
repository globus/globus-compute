from __future__ import annotations

import importlib
import inspect
import logging
import textwrap
import typing as t
from enum import Enum

from globus_compute_sdk.errors import (
    DeserializationError,
    SerdeError,
    SerializationError,
)
from globus_compute_sdk.serialize.base import IDENTIFIER_LENGTH, SerializationStrategy
from globus_compute_sdk.serialize.concretes import (
    DEFAULT_STRATEGY_CODE,
    DEFAULT_STRATEGY_DATA,
    SELECTABLE_STRATEGIES,
)

logger = logging.getLogger(__name__)


class AllowlistWildcard(str, Enum):
    """
    Special values that can be included in deserializer allowlists to allow
    all serializers of a certain kind.
    """

    #: Include this to allow all *code* serializers from the Compute SDK.
    CODE = "globus_compute_sdk.*Code"
    #: Include this to allow all *data* serializers from the Compute SDK.
    DATA = "globus_compute_sdk.*Data"


DeserializerAllowlist = t.Iterable[
    t.Union[type[SerializationStrategy], str, AllowlistWildcard]
]


def assert_strategy_type_valid(
    strategy_type: type[SerializationStrategy], for_code: bool
) -> None:
    if strategy_type not in SELECTABLE_STRATEGIES:
        raise SerdeError(
            f"{strategy_type.__name__} is not a known serialization strategy"
            f" (must be one of {SELECTABLE_STRATEGIES})"
        )

    if strategy_type.for_code != for_code:
        etype = "code" if for_code else "data"
        gtype = "code" if strategy_type.for_code else "data"
        raise SerdeError(
            f"{strategy_type.__name__} is a {gtype} serialization strategy,"
            f" expected a {etype} strategy"
        )


def validate_strategy(
    strategy: SerializationStrategy, for_code: bool
) -> SerializationStrategy:
    assert_strategy_type_valid(type(strategy), for_code)
    return strategy


def parse_allowlist(
    unvalidated: DeserializerAllowlist | None,
) -> set[type[SerializationStrategy]]:
    if not unvalidated:
        return set()

    validated: set[type[SerializationStrategy]] = set()
    wildcards: set[AllowlistWildcard] = set()
    for value in unvalidated:
        try:
            AllowlistWildcard(value)
        except ValueError:
            pass
        else:
            # value is either an instance of AllowlistWildcard or the .value of
            # such an instance. mypy is not smart enough to know that
            wildcards.add(value)  # type: ignore
            continue

        resolved_strategy_class = None
        if isinstance(value, str):
            try:
                mod_name, class_name = value.rsplit(".", 1)
                mod = importlib.import_module(mod_name)
                resolved_strategy_class = getattr(mod, class_name)
            except Exception as e:
                raise SerdeError(f"`{value}` is not a valid path to a strategy") from e
        else:
            resolved_strategy_class = value

        if not inspect.isclass(resolved_strategy_class) or not issubclass(
            resolved_strategy_class, SerializationStrategy
        ):
            raise SerdeError(
                "Allowed deserializers must either be SerializationStrategies"
                f" or valid paths to them (got {value})"
            )

        assert_strategy_type_valid(
            resolved_strategy_class, resolved_strategy_class.for_code
        )
        validated.add(resolved_strategy_class)

    for wildcard in wildcards:
        for_code = wildcard == AllowlistWildcard.CODE
        if any(s.for_code == for_code for s in validated):
            raise SerdeError(
                f"Cannot mix '{wildcard}' with specific deserializers"
                f" (got {list(unvalidated)})"
            )
        validated.update(s for s in SELECTABLE_STRATEGIES if s.for_code == for_code)

    if len({s.for_code for s in validated}) != 2:
        raise SerdeError(
            "Deserialization allowlists must contain at least one code and one data"
            f" deserializer/wildcard (got: {list(unvalidated)})"
        )

    return validated


class ComputeSerializer:
    def __init__(
        self,
        strategy_code: SerializationStrategy | None = None,
        strategy_data: SerializationStrategy | None = None,
        *,
        allowed_deserializer_types: DeserializerAllowlist | None = None,
    ):
        """
        Provides a uniform interface to Compute's various serialization strategies.

        :param strategy_code: The serialization strategy for code. If not supplied, uses
            :const:`~globus_compute_sdk.serialize.DEFAULT_STRATEGY_CODE`.
        :param strategy_data: The serialization strategy for data. If not supplied, uses
            :const:`~globus_compute_sdk.serialize.DEFAULT_STRATEGY_DATA`.
        :param allowed_deserializers: A list of strategies that are allowed to
            deserialize data/code, in the form of
            :class:`~globus_compute_sdk.serialize.base.SerializationStrategy` subclasses
            (not instances of those classes!), import paths to those classes, or the
            special :class:`AllowlistWildcard` values. Requires at least one code and
            one data strategy to be specified. If falsy, all deserializers are allowed.
        :raises: If any serializer is unknown to Compute, or if any of the arguments are
            not in the expected format.
        """

        self.code_serializer = validate_strategy(
            strategy_code or DEFAULT_STRATEGY_CODE, True
        )
        self.data_serializer = validate_strategy(
            strategy_data or DEFAULT_STRATEGY_DATA, False
        )

        self.allowed_deserializer_types = parse_allowlist(allowed_deserializer_types)

    def serialize(self, data: t.Any) -> str:
        """
        Converts Python data to a string representation, using this serializer's
        configured strategies.

        :param data: Any Python object that can be serialized by this serializer's
            strategies. An error is raised if this object is incompatible.

        :returns: The string representation, which can be deserialized by any
            :class:`ComputeSerializer`.

        :raises: If this serializer's strategy is unable to serialize this data.
        """
        if callable(data):
            stype, strategy = "Code", self.code_serializer
        else:
            stype, strategy = "Data", self.data_serializer

        try:
            return strategy.serialize(data)
        except Exception as e:
            err_msg = f"{stype} serialization strategy {type(strategy).__name__} failed"
            raise SerializationError(err_msg) from e

    def deserialize(self, payload: str) -> t.Any:
        """
        Converts a string representation of a Python object generated by a
        :class:`ComputeSerializer` back to the object it represents. Inverse
        of :func:`serialize`.

        :param payload: Serialized data to turn back into Python data.

        :returns: The Python data represented by that string.

        :raises: If the payload is in the wrong format, or if it was not serialized
            with an allowed strategy.
        """
        header = payload[:IDENTIFIER_LENGTH]
        strategy = SerializationStrategy.get_cached(header)

        if not strategy:
            raise DeserializationError(f"Invalid header: {header} in data payload")

        self.assert_deserializer_allowed(strategy)

        return strategy.deserialize(payload)

    @staticmethod
    def pack_buffers(buffers: list[str]) -> str:
        """
        Combines a list of Compute-serialized buffers into a single string format
        understood by the :class:`ComputeSerializer`.

        :param buffers: A list of serialized buffers, as produced by :func:`serialize`.
        """
        packed = ""
        for buf in buffers:
            s_length = str(len(buf)) + "\n"
            packed += s_length + buf

        return packed

    @staticmethod
    def unpack_buffers(packed_buffer: str) -> list[str]:
        """
        Splits a packed buffer string into its constituents.
        Inverse of :func:`pack_buffers`.

        :param packed_buffer: A packed buffer string as produced by
            :func:`pack_buffers`.
        """
        unpacked = []
        while packed_buffer:
            s_length, buf = packed_buffer.split("\n", 1)
            i_length = int(s_length)
            current, packed_buffer = buf[:i_length], buf[i_length:]
            unpacked.extend([current])

        return unpacked

    def unpack_and_deserialize(self, packed_buffer: str) -> list[t.Any]:
        """
        Takes a packed string containing three buffers, unpacks them, and deserializes.

        :param packed_buffer: A packed buffer string as produced by
            :func:`pack_buffers`.

        :raises: If ``packed_buffers`` does not contain exactly three buffers.
        """
        unpacked = []
        while packed_buffer:
            s_length, buf = packed_buffer.split("\n", 1)
            i_length = int(s_length)
            current, packed_buffer = buf[:i_length], buf[i_length:]
            deserialized = self.deserialize(current)
            unpacked.extend([deserialized])

        assert len(unpacked) == 3, "Unpack expects 3 buffers, got {}".format(
            len(unpacked)
        )

        return unpacked

    def check_strategies(
        self, function: t.Callable, *args: t.Any, **kwargs: t.Any
    ) -> list[t.Any]:
        """
        Check that the given function, args, and kwargs are compatible with this
        :class:`ComputeSerializer`'s serialization strategies.

        Uses the same interface as the :class:`~globus_compute_sdk.Executor`'s
        :func:`~globus_compute_sdk.Executor.submit` method. Returns a list containing
        the function, args, and kwargs after going through serialization and
        deserialization.

        For example:

        .. code-block:: python

            def function(x, y=None):
                return x+y

            serde = ComputeSerializer()

            rf, args, kwargs = serde.check_strategies(function, 3, y=4)
            assert rf(*args, **kwargs) == 7
        """

        try:
            ser_fn = self.serialize(function)
            ser_args = self.serialize(args)
            ser_kwargs = self.serialize(kwargs)

            packed = self.pack_buffers([ser_fn, ser_args, ser_kwargs])
        except Exception as e:
            raise SerializationError("check_strategies failed to serialize") from e

        try:
            return self.unpack_and_deserialize(packed)
        except Exception as e:
            raise DeserializationError("check_strategies failed to deserialize") from e

    def assert_deserializer_allowed(self, strategy: SerializationStrategy) -> None:
        """
        Raise an error if there is a configured deserializer allowlist and the given
        :class:`~globus_compute_sdk.serialize.base.SerializationStrategy` is not an
        instance of any type in that allowlist.

        :param strategy: The strategy that was requested for deserialization.

        :raises: If ``strategy`` is not allowed in this serializer.
        """

        if (
            not self.allowed_deserializer_types
            or type(strategy) in self.allowed_deserializer_types
        ):
            return

        stype = "Code" if strategy.for_code else "Data"
        payload_type = "function" if strategy.for_code else "arguments"
        allowed_names = ", ".join(
            sorted(t.__name__ for t in self.allowed_deserializer_types)
        )
        msg = (
            f"\n{stype} serializer {type(strategy).__name__} disabled by current"
            f" configuration.\nThe current configuration requires the *{payload_type}*"
            f" to be serialized with one of the allowed {stype} classes:\n\n"
            f"    Allowed serializers: {allowed_names}"
            # note that there is (intentionally) no link to the documentation in this
            # error message - that's because the SDK appends its own hint to any
            # apparent serialization errors coming back from the endpoint. see https://github.com/globus/globus-compute/blob/112dc3ae9d9986f36618976f8806f0bd48702460/compute_sdk/globus_compute_sdk/errors/error_types.py#L74  # noqa: E501
        )

        raise DeserializationError(textwrap.indent(msg, " "))
