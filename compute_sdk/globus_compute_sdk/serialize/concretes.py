from __future__ import annotations

import codecs
import inspect
import json
import logging
import pickle
import typing as t
from collections import OrderedDict

import dill
from globus_compute_sdk.errors import DeserializationError, SerializationError
from globus_compute_sdk.serialize.base import SerializationStrategy

logger = logging.getLogger(__name__)


class DillDataBase64(SerializationStrategy):
    """
    Serializes Python data to binary via |dill.dumps|_, and then
    encodes that binary to a string using base 64 representation.
    """

    identifier = "00\n"  #:
    for_code = False  #:

    def __init__(self):
        super().__init__()

    def serialize(self, data: t.Any) -> str:
        ":meta private:"
        x = codecs.encode(dill.dumps(data), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload: str) -> t.Any:
        ":meta private:"
        chomped = self.chomp(payload)
        data = dill.loads(codecs.decode(chomped.encode(), "base64"))
        return data


class JSONData(SerializationStrategy):
    """
    Serializes Python data to a string via :func:`json.dumps`.
    """

    identifier = "11\n"  #:
    for_code = False  #:

    def __init__(self):
        super().__init__()

    def serialize(self, data: t.Any) -> str:
        ":meta private:"
        return self.identifier + json.dumps(data)

    def deserialize(self, payload: str) -> t.Any:
        ":meta private:"
        chomped = self.chomp(payload)
        data = json.loads(chomped)
        return data


class DillCodeSource(SerializationStrategy):
    """
    Extracts a function's body via |dill.getsource|_, serializes it
    with |dill.dumps|_, then encodes it via base 64.

    .. list-table:: Supports
        :header-rows: 1
        :align: center

        * - Functions defined in Python interpreter
          - Code from notebooks
          - Interop between mismatched Python versions
          - Decorated functions
        * - ✅
          - ❌
          - ⚠️
          - ❌
    """

    identifier = "04\n"  #:
    for_code = True  #:

    def __init__(self):
        super().__init__()

    def serialize(self, data) -> str:
        ":meta private:"
        name = data.__name__
        body = dill.source.getsource(data, lstrip=True)
        x = codecs.encode(dill.dumps((name, body)), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload: str):
        ":meta private:"
        chomped = self.chomp(payload)
        name, body = dill.loads(codecs.decode(chomped.encode(), "base64"))
        exec_ns: dict = {}
        exec(body, exec_ns)
        return exec_ns[name]


class DillCodeTextInspect(SerializationStrategy):
    """
    Extracts a function's body via :func:`inspect.getsource`, serializes it
    with |dill.dumps|_, then encodes it via base 64.

    .. list-table:: Supports
        :header-rows: 1
        :align: center

        * - Functions defined in Python interpreter
          - Code from notebooks
          - Interop between mismatched Python versions
          - Decorated functions
        * - ❌
          - ✅
          - ⚠️
          - ❌
    """

    identifier = "03\n"  #:
    for_code = True  #:

    def __init__(self):
        super().__init__()

    def serialize(self, data) -> str:
        ":meta private:"
        name = data.__name__
        body = inspect.getsource(data)
        x = codecs.encode(dill.dumps((name, body)), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload: str):
        ":meta private:"
        chomped = self.chomp(payload)
        name, body = dill.loads(codecs.decode(chomped.encode(), "base64"))
        exec_ns: dict = {}
        exec(body, exec_ns)
        return exec_ns[name]


class PickleCode(SerializationStrategy):
    """
    This strategy is deprecated and new submissions cannot use it to
    serialize functions. Very old functions that were serialized with
    this can still be deserialized.

    :meta private:
    """

    _DEPRECATED = True

    identifier = "02\n"  #:
    for_code = True  #:

    def __init__(self):
        super().__init__()

    def serialize(self, data) -> str:
        ":meta private:"
        raise NotImplementedError("Pickle serialization is no longer supported")
        # x = codecs.encode(pickle.dumps(data), "base64").decode()
        # return self.identifier + x

    def deserialize(self, payload: str):
        ":meta private:"
        chomped = self.chomp(payload)
        data = pickle.loads(codecs.decode(chomped.encode(), "base64"))
        return data


class DillCode(SerializationStrategy):
    """
    Directly serializes the function via |dill.dumps|_, then encodes it in base 64.

    .. list-table:: Supports
        :header-rows: 1
        :align: center

        * - Functions defined in Python interpreter
          - Code from notebooks
          - Interop between mismatched Python versions
          - Decorated functions
        * - ❌
          - ✅
          - ❌
          - ✅
    """

    identifier = "01\n"  #:
    for_code = True  #:

    def __init__(self):
        super().__init__()

    def serialize(self, data) -> str:
        ":meta private:"
        x = codecs.encode(dill.dumps(data), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload: str):
        ":meta private:"
        chomped = self.chomp(payload)
        function = dill.loads(codecs.decode(chomped.encode(), "base64"))
        return function


class PureSourceTextInspect(SerializationStrategy):
    """
    Extracts a function's definition via :func:`inspect.getsource` and sends the
    resulting string directly over the wire.

    .. list-table:: Supports
        :header-rows: 1
        :align: center

        * - Functions defined in Python interpreter
          - Code from notebooks
          - Interop between mismatched Python versions
          - Decorated functions
        * - ❌
          - ✅
          - ⚠️
          - ❌
    """

    # "Source TextInspect"
    identifier = "st\n"  #:
    for_code = True  #:

    _separator = ":"

    def serialize(self, data) -> str:
        ":meta private:"
        name = data.__name__
        body = inspect.getsource(data)
        x = name + self._separator + body
        return self.identifier + x

    def deserialize(self, payload: str):
        ":meta private:"
        chomped = self.chomp(payload)
        name, body = chomped.split(self._separator, 1)
        exec_ns: dict = {}
        exec(body, exec_ns)
        return exec_ns[name]


class PureSourceDill(SerializationStrategy):
    """
    Extracts a function's definition via |dill.getsource|_ and sends the resulting
    string directly over the wire.

    .. list-table:: Supports
        :header-rows: 1
        :align: center

        * - Functions defined in Python interpreter
          - Code from notebooks
          - Interop between mismatched Python versions
          - Decorated functions
        * - ✅
          - ❌
          - ⚠️
          - ❌
    """

    # "Source Dill"
    identifier = "sd\n"  #:
    for_code = True  #:

    _separator = ":"

    def serialize(self, data) -> str:
        ":meta private:"
        name = data.__name__
        body = dill.source.getsource(data, lstrip=True)
        x = name + self._separator + body
        return self.identifier + x

    def deserialize(self, payload: str):
        ":meta private:"
        chomped = self.chomp(payload)
        name, body = chomped.split(self._separator, 1)
        exec_ns: dict = {}
        exec(body, exec_ns)
        return exec_ns[name]


class CombinedCode(SerializationStrategy):
    """
    This strategy attempts to serialize the function using the other code strategies
    (namely :class:`DillCodeSource`, :class:`DillCode`, :class:`DillCodeTextInspect`,
    :class:`PureSourceDill`, and :class:`PureSourceTextInspect`), combining the results
    of each sub-strategy as the final serialized payload. When deserializing, it tries
    each sub-strategy in order until one successfully deserializes its part of the
    payload. Intended for maximum compatibility, but is slower than using a single
    strategy and generates larger payloads.

    .. list-table:: Supports
        :header-rows: 1
        :align: center

        * - Functions defined in Python interpreter
          - Code from notebooks
          - Interop between mismatched Python versions
          - Decorated functions
        * - ✅
          - ✅
          - ⚠️
          - ✅
    """

    identifier = "10\n"  #:
    for_code = True  #:

    # Functions are serialized using the following strategies and the resulting encoded
    # versions are stored as chunks.  Allows redundancy if one of the strategies fails
    # on deserialization (undetectable during previous serialization attempts).
    # Note that, of the 3 categories of failures:
    #   1) Failure on serialization
    #   2) Failure on deserialization
    #   3) Failure on running the strategy
    # We have agreed that we can only assist with 1) and 2).
    _chunk_strategies = OrderedDict(
        [
            (DillCodeSource.identifier, DillCodeSource),
            (DillCode.identifier, DillCode),
            (DillCodeTextInspect.identifier, DillCodeTextInspect),
            (PureSourceDill.identifier, PureSourceDill),
            (PureSourceTextInspect.identifier, PureSourceTextInspect),
        ]
    )

    _separator = ":"

    def get_multiple_payloads(self, payload: str) -> tuple[tuple[str, str], ...]:
        parts: list[str] = self.chomp(payload).split(CombinedCode._separator)
        assert len(parts) > 0
        ai = iter(parts)

        # ie. ((04\n', 'gACQ...'), ('01\n', 'sAAbk2...'), ...)
        return tuple(zip(ai, ai))

    def serialize(self, data) -> str:
        ":meta private:"
        chunks = []
        for id, cls in CombinedCode._chunk_strategies.items():
            strategy = cls()
            try:
                serialized = strategy.serialize(data)
            except Exception:
                # Ignore and move on to next
                continue
            id_length = len(id)
            chunks.append(serialized[:id_length])
            chunks.append(serialized[id_length:])

        if len(chunks) == 0:
            raise SerializationError("No serialization methods were successful")

        return self.identifier + CombinedCode._separator.join(chunks)

    def deserialize(self, payload: str, variation: int = 0) -> t.Any:
        """
        If a variation is specified, try that strategy/payload if
        present.
        If variation is not specified, loop and return the first
        one that deserializes successfully.  If none works, raise

        Note variation is 1-indexed (ie. 2 means 2nd strategy), not
        the ``identifier`` of the strategy (like ``"04\\n"``, ``"01\\n"``).
        """
        count = 0
        for serial_id, encoded_func in self.get_multiple_payloads(payload):
            count += 1
            if variation == 0 or count == variation:
                if serial_id in CombinedCode._chunk_strategies:
                    strategy = CombinedCode._chunk_strategies[serial_id]()
                    try:
                        return strategy.deserialize(serial_id + encoded_func)
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


SELECTABLE_STRATEGIES = [
    t.__class__
    for t in SerializationStrategy._CACHE.values()
    if not getattr(t, "_DEPRECATED", False)
]

#: The *code* serialization strategy used by :class:`ComputeSerializer`
#: when one is not specified.
DEFAULT_STRATEGY_CODE = SerializationStrategy.get_cached_by_class(DillCode)
#: The *data* serialization strategy used by :class:`ComputeSerializer`
#: when one is not specified.
DEFAULT_STRATEGY_DATA = SerializationStrategy.get_cached_by_class(DillDataBase64)
