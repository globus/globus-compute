from __future__ import annotations

import importlib
import logging
import typing as t

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
    STRATEGIES_MAP,
)

logger = logging.getLogger(__name__)

DeserializerAllowlist = t.Iterable[t.Union[type[SerializationStrategy], str]]


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


def validate_allowlist(
    unvalidated: DeserializerAllowlist, for_code: bool
) -> set[type[SerializationStrategy]]:
    validated = set()
    for value in unvalidated:
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

        if not issubclass(resolved_strategy_class, SerializationStrategy):
            raise SerdeError(
                "Allowed deserializers must either be SerializationStrategies"
                f" or valid paths to them (got {value})"
            )

        assert_strategy_type_valid(resolved_strategy_class, for_code)
        validated.add(resolved_strategy_class)

    return validated


class ComputeSerializer:
    """Provides uniform interface to underlying serialization strategies"""

    def __init__(
        self,
        strategy_code: SerializationStrategy | None = None,
        strategy_data: SerializationStrategy | None = None,
        *,
        allowed_code_deserializer_types: DeserializerAllowlist | None = None,
        allowed_data_deserializer_types: DeserializerAllowlist | None = None,
    ):
        """Instantiate the appropriate classes"""

        self.code_serializer = validate_strategy(
            strategy_code or DEFAULT_STRATEGY_CODE, True
        )
        self.data_serializer = validate_strategy(
            strategy_data or DEFAULT_STRATEGY_DATA, False
        )
        self.allowed_code_deserializer_types = validate_allowlist(
            allowed_code_deserializer_types or [], True
        )
        self.allowed_data_deserializer_types = validate_allowlist(
            allowed_data_deserializer_types or [], False
        )

        self.header_size = IDENTIFIER_LENGTH

        self.strategies = {
            header: strategy_class()
            for header, strategy_class in STRATEGIES_MAP.items()
        }

    def serialize(self, data):
        if callable(data):
            stype, strategy = "Code", self.code_serializer
        else:
            stype, strategy = "Data", self.data_serializer

        try:
            return strategy.serialize(data)
        except Exception as e:
            err_msg = f"{stype} serialization strategy {type(strategy).__name__} failed"
            raise SerializationError(err_msg) from e

    def deserialize(self, payload):
        """
        Parameters
        ----------
        payload : str
           Payload object to be deserialized

        """
        header = payload[0 : self.header_size]
        strategy = self.strategies.get(header)

        if not strategy:
            raise DeserializationError(f"Invalid header: {header} in data payload")

        self.assert_deserializer_allowed(strategy)

        return strategy.deserialize(payload)

    @staticmethod
    def pack_buffers(buffers):
        """
        Parameters
        ----------
        buffers : list of \n terminated strings
        """
        packed = ""
        for buf in buffers:
            s_length = str(len(buf)) + "\n"
            packed += s_length + buf

        return packed

    @staticmethod
    def unpack_buffers(packed_buffer):
        """
        Parameters
        ----------
        packed_buffer : packed buffer as string
        """
        unpacked = []
        while packed_buffer:
            s_length, buf = packed_buffer.split("\n", 1)
            i_length = int(s_length)
            current, packed_buffer = buf[:i_length], buf[i_length:]
            unpacked.extend([current])

        return unpacked

    def unpack_and_deserialize(self, packed_buffer):
        """Unpacks a packed buffer and returns the deserialized contents
        Parameters
        ----------
        packed_buffer : packed buffer as string
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

    def check_strategies(self, function: t.Callable, *args, **kwargs):
        """
        Check that the given function, args, and kwargs are compatible with this
        ComputeSerializer's serialization strategies.

        Uses the same interface as Executor.submit. Returns a list containing the
        function, args, and kwargs after going through serialization and
        deserialization.
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
        allowlist = (
            self.allowed_code_deserializer_types
            if strategy.for_code
            else self.allowed_data_deserializer_types
        )

        if not allowlist or type(strategy) in allowlist:
            return

        allowed_names = [t.__name__ for t in allowlist]
        dtype = "Code" if strategy.for_code else "Data"
        htype = "function" if strategy.for_code else "arguments"
        help_url = "https://globus-compute.readthedocs.io/en/stable/sdk.html#specifying-a-serialization-strategy"  # noqa

        raise DeserializationError(
            f"{dtype} deserializer {type(strategy).__name__} is not allowed; expected "
            f"one of {allowed_names}. (Hint: reserialize the {htype} with one of the "
            f"allowed serialization strategies and resubmit. For more information, see "
            f"{help_url}.)"
        )
