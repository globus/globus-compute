import codecs
import inspect
import logging
import pickle

import dill

from funcx.serialize.base import fxPicker_shared

logger = logging.getLogger(__name__)


class pickle_base64(fxPicker_shared):

    _identifier = "00\n"
    _for_code = False

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = codecs.encode(pickle.dumps(data), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = pickle.loads(codecs.decode(chomped.encode(), "base64"))
        return data


class code_dill_source(fxPicker_shared):
    """This method uses dill's getsource method to extract the function body and
    then serializes it.

    Code from interpretor/main        : Yes
    Code from notebooks               : No
    Works with mismatching py versions: Yes
    Decorated fns                     : No
    """

    _identifier = "04\n"
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        name = data.__name__
        body = dill.source.getsource(data)
        x = codecs.encode(pickle.dumps((name, body)), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        name, body = pickle.loads(codecs.decode(chomped.encode(), "base64"))
        exec(body)
        return locals()[name]


class code_text_inspect(fxPicker_shared):
    """This method uses the inspect library to extract the function body and
    then serializes it.

    Code from interpretor/main        : ?
    Code from notebooks               : Yes
    Works with mismatching py versions: Yes
    Decorated fns                     : No
    """

    _identifier = "03\n"
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        name = data.__name__
        body = inspect.getsource(data)
        x = codecs.encode(pickle.dumps((name, body)), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        name, body = pickle.loads(codecs.decode(chomped.encode(), "base64"))
        exec(body)
        return locals()[name]


class code_dill(fxPicker_shared):
    """This method uses dill to directly serialize a function.

    Code from interpretor/main        : No
    Code from notebooks               : Yes
    Works with mismatching py versions: No
    Decorated fns                     : Yes
    """

    _identifier = "01\n"
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = codecs.encode(dill.dumps(data), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        function = dill.loads(codecs.decode(chomped.encode(), "base64"))
        return function


class code_pickle(fxPicker_shared):
    """This method uses pickle to directly serialize a function.
    Could be deprecated in favor of just using dill, but pickle is a little bit
    faster.

    Code from interpretor/main        : No
    Code from notebooks               : Yes
    Works with mismatching py versions: No
    Decorated fns                     : Yes
    """

    _identifier = "02\n"
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = codecs.encode(pickle.dumps(data), "base64").decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = pickle.loads(codecs.decode(chomped.encode(), "base64"))
        return data


def ensure_all_concrete_serializers_registered():
    """
    This function ensures that all of the concrete serializers have been
    registered.

    In fact, it is a no-op because concrete serializers are registered
    automatically via __init_subclass__, but importing and running this
    function serves as a neatly semantic assertion that all of the
    de/serialization classes have been initialized.
    """
