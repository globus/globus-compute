from __future__ import annotations

import logging

from globus_compute_sdk.serialize.base import SerializationStrategy, SerializerError
from globus_compute_sdk.serialize.concretes import (
    DEFAULT_STRATEGY_CODE,
    DEFAULT_STRATEGY_DATA,
    SELECTABLE_STRATEGIES,
    STRATEGIES_MAP,
)

logger = logging.getLogger(__name__)


class ComputeSerializer:
    """Provides uniform interface to underlying serialization strategies"""

    def __init__(
        self,
        strategy_code: SerializationStrategy | None = None,
        strategy_data: SerializationStrategy | None = None,
    ):
        """Instantiate the appropriate classes"""

        def validate(strategy: SerializationStrategy) -> SerializationStrategy:
            if type(strategy) not in SELECTABLE_STRATEGIES:
                raise SerializerError(
                    f"{strategy} is not a known serialization strategy "
                    f"(must be one of {SELECTABLE_STRATEGIES})"
                )

            return strategy

        self.strategy_code = (
            validate(strategy_code) if strategy_code else DEFAULT_STRATEGY_CODE
        )
        self.strategy_data = (
            validate(strategy_data) if strategy_data else DEFAULT_STRATEGY_DATA
        )

        # grab a randomish ID from the map (all identifiers should be the same length)
        identifier = next(iter(STRATEGIES_MAP.keys()))
        self.header_size = len(identifier)

        self.strategies = {
            header: strategy_class()
            for header, strategy_class in STRATEGIES_MAP.items()
        }

    def serialize(self, data):
        if callable(data):
            stype, strategy = "Callable", self.strategy_code
        else:
            stype, strategy = "Data", self.strategy_data

        try:
            return strategy.serialize(data)
        except Exception as e:
            err_msg = f"{stype} Serialization strategy {strategy} failed"
            raise SerializerError(err_msg) from e

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
            raise SerializerError(f"Invalid header: {header} in data payload")

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
