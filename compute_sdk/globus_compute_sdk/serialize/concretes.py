from __future__ import annotations

import codecs
import inspect
import logging
import pickle
from collections import OrderedDict

import dill

from funcx.serialize.base import DeserializationError, SerializeBase

logger = logging.getLogger(__name__)


class DillDataBase64(SerializeBase):
    identifier = "00\n"
    _for_code = False

    def __init__(self):
        super().__init__()

    def serialize(self, data) -> str:
        x = codecs.encode(dill.dumps(data), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload: str):
        chomped = self.chomp(payload)
        data = dill.loads(codecs.decode(chomped.encode(), "base64"))
        return data


class DillCodeSource(SerializeBase):
    """This method uses dill's getsource method to extract the function body and
    then serializes it.

    Code from interpreter/main        : Yes
    Code from notebooks               : No
    Works with mismatching py versions: Yes
    Decorated fns                     : No
    """

    identifier = "04\n"
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data) -> str:
        name = data.__name__
        body = dill.source.getsource(data, lstrip=True)
        x = codecs.encode(dill.dumps((name, body)), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload: str):
        chomped = self.chomp(payload)
        name, body = dill.loads(codecs.decode(chomped.encode(), "base64"))
        exec(body)
        return locals()[name]


class DillCodeTextInspect(SerializeBase):
    """This method uses the inspect library to extract the function body and
    then serializes it.

    Code from interpreter/main        : ?
    Code from notebooks               : Yes
    Works with mismatching py versions: Yes
    Decorated fns                     : No
    """

    identifier = "03\n"
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data) -> str:
        name = data.__name__
        body = inspect.getsource(data)
        x = codecs.encode(dill.dumps((name, body)), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload: str):
        chomped = self.chomp(payload)
        name, body = dill.loads(codecs.decode(chomped.encode(), "base64"))
        exec(body)
        return locals()[name]


class PickleCode(SerializeBase):
    """
    Deprecated in favor of just using dill, but deserialization is
    supported for legacy functions
    Code from interpreter/main        : No
    Code from notebooks               : Yes
    Works with mismatching py versions: No
    Decorated fns                     : Yes
    """

    identifier = "02\n"
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data) -> str:
        raise NotImplementedError("Pickle serialization is no longer supported")
        # x = codecs.encode(pickle.dumps(data), "base64").decode()
        # return self.identifier + x

    def deserialize(self, payload: str):
        chomped = self.chomp(payload)
        data = pickle.loads(codecs.decode(chomped.encode(), "base64"))
        return data


class DillCode(SerializeBase):
    """This method uses dill to directly serialize a function.

    Code from interpreter/main        : No
    Code from notebooks               : Yes
    Works with mismatching py versions: No
    Decorated fns                     : Yes
    """

    identifier = "01\n"

    def __init__(self):
        super().__init__()

    def serialize(self, data) -> str:
        x = codecs.encode(dill.dumps(data), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload: str):
        chomped = self.chomp(payload)
        function = dill.loads(codecs.decode(chomped.encode(), "base64"))
        return function


class CombinedCode(SerializeBase):
    """This method uses multiple methods to serialize a function

    # These should be a superset of the methods
    Code from interpreter/main        : Yes
    Code from notebooks               : Yes
    Works with mismatching py versions: Yes
    Decorated fns                     : Yes
    """

    identifier = "10\n"

    def get_multiple_payloads(self, payload: str) -> tuple[tuple[str, str], ...]:
        parts: list[str] = self.chomp(payload).split(COMBINED_SEPARATOR)
        assert len(parts) == 2 * len(COMBINED_SERIALIZE_METHODS)
        ai = iter(parts)

        # ie. ((04\n', 'gACQ...'), ('01\n', 'sAAbk2...'), ...)
        return tuple(zip(ai, ai))

    def serialize(self, data) -> str:
        chunks = []
        for id in COMBINED_SERIALIZE_METHODS:
            method = METHODS_MAP_CODE[id]()
            serialized = method.serialize(data)
            id_length = len(id)
            chunks.append(serialized[:id_length])
            chunks.append(serialized[id_length:])
        return self.identifier + COMBINED_SEPARATOR.join(chunks)

    def deserialize(self, payload: str, variation: int = 0):
        """
        If a variation is specified, try that method/payload if
        present.
        If variation is not specified, loop and return the first
        one that deserializes successfully.  If none works, raise

        Note variation is 1-indexed (ie. 2 means 2nd method), not
        the .identifier/serial_id of the method like 04\n or 01\n
        """
        count = 0
        for serial_id, encoded_func in self.get_multiple_payloads(payload):
            count += 1
            if variation == 0 or count == variation:
                if serial_id in METHODS_MAP_CODE:
                    deserialize_method = METHODS_MAP_CODE[serial_id]()
                    try:
                        return deserialize_method.deserialize(serial_id + encoded_func)
                    except Exception:
                        if variation > 0:
                            # Only raise if this variation was specified and fails
                            #  otherwise, just try next available
                            raise DeserializationError(
                                f"Failed to deserialize using variation #{count}"
                            )
                else:
                    raise DeserializationError(f"Invalid serialization id {serial_id}")
        raise DeserializationError(f"Deserialization failed after {count} tries")


"""
Functions are serialized using the follow methods and
the resulting encoded versions are stored.  Redundancy if
one of the methods fail on deserlization, undetectable during
serialization attempts previously.

Note that of the 3 categories of failures:
  1) Failure on serialization (previously handled)
  2) Failure on deserialization (currently and with off_process_checker)
  3) Failure on running the method (we can't do much about this type)

  We have agreed that we can only assist with 1) and 2)
"""
COMBINED_SERIALIZE_METHODS = [
    DillCodeSource.identifier,
    DillCode.identifier,
]

# Used to separate base64 encoded serialized chunks of methods
COMBINED_SEPARATOR = ":"


METHODS_MAP_CODE = OrderedDict(
    [
        (DillCodeSource.identifier, DillCodeSource),
        (DillCode.identifier, DillCode),
        (DillCodeTextInspect.identifier, DillCodeTextInspect),
        (PickleCode.identifier, PickleCode),
        (CombinedCode.identifier, CombinedCode),
    ]
)


METHODS_MAP_DATA = OrderedDict(
    [
        (DillDataBase64.identifier, DillDataBase64),
    ]
)
