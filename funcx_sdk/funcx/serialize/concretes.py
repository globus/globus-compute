import codecs
import inspect
import pickle
from collections import OrderedDict

import dill

from funcx.serialize.base import SerializeBase


class DillDataBase64(SerializeBase):
    identifier = "00\n"
    _for_code = False

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = codecs.encode(dill.dumps(data), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload):
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

    def serialize(self, data):
        name = data.__name__
        body = dill.source.getsource(data, lstrip=True)
        x = codecs.encode(dill.dumps((name, body)), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload):
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

    def serialize(self, data):
        name = data.__name__
        body = inspect.getsource(data)
        x = codecs.encode(dill.dumps((name, body)), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload):
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

    def serialize(self, data):
        raise NotImplementedError("Pickle serialization is no longer supported")
        # x = codecs.encode(pickle.dumps(data), "base64").decode()
        # return self.identifier + x

    def deserialize(self, payload):
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

    def serialize(self, data):
        x = codecs.encode(dill.dumps(data), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        function = dill.loads(codecs.decode(chomped.encode(), "base64"))
        return function


METHODS_MAP_CODE = OrderedDict(
    [
        (DillCodeSource.identifier, DillCodeSource),
        (DillCode.identifier, DillCode),
        (DillCodeTextInspect.identifier, DillCodeTextInspect),
        (PickleCode.identifier, PickleCode),
    ]
)

METHODS_MAP_DATA = OrderedDict(
    [
        (DillDataBase64.identifier, DillDataBase64),
    ]
)
