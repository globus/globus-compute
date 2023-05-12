from __future__ import annotations

import logging

from globus_compute_sdk.serialize.base import SerializeBase, SerializerError
from globus_compute_sdk.serialize.concretes import (
    DEFAULT_METHOD_CODE,
    DEFAULT_METHOD_DATA,
    METHODS_MAP,
    SELECTABLE_SERIALIZERS,
)

logger = logging.getLogger(__name__)


class ComputeSerializer:
    """Wraps several serializers for one uniform interface"""

    def __init__(
        self,
        method_code: SerializeBase | None = None,
        method_data: SerializeBase | None = None,
    ):
        """Instantiate the appropriate classes"""

        def validate(method: SerializeBase) -> SerializeBase:
            if type(method) not in SELECTABLE_SERIALIZERS:
                raise SerializerError(
                    f"{method} is not a known serializer "
                    f"(must be one of {SELECTABLE_SERIALIZERS})"
                )

            return method

        self.method_code = validate(method_code) if method_code else DEFAULT_METHOD_CODE
        self.method_data = validate(method_data) if method_data else DEFAULT_METHOD_DATA

        # grab a randomish ID from the map (all identifiers should be the same length)
        identifier = next(iter(METHODS_MAP.keys()))
        self.header_size = len(identifier)

        self.methods = {
            header: method_class() for header, method_class in METHODS_MAP.items()
        }

    def serialize(self, data):
        if callable(data):
            stype, method = "Callable", self.method_code
        else:
            stype, method = "Data", self.method_data

        try:
            return method.serialize(data)
        except Exception as e:
            err_msg = f"{stype} Serialization Method {method} failed"
            raise SerializerError(err_msg) from e

    def deserialize(self, payload):
        """
        Parameters
        ----------
        payload : str
           Payload object to be deserialized

        """
        header = payload[0 : self.header_size]
        method = self.methods.get(header)

        if not method:
            raise SerializerError(f"Invalid header: {header} in data payload")

        return method.deserialize(payload)

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
