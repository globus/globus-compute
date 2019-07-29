from abc import ABCMeta, abstractmethod, abstractproperty
import codecs
import json
import dill
import pickle
import inspect

class fxPicker_enforcer(metaclass=ABCMeta):

    @abstractmethod
    def serialize(self, data):
        pass

    @abstractmethod
    def deserialize(self, payload):
        pass

class fxPicker_shared(object):

    @property
    def identifier(self):
        """ Get the identifier of the serialization method

        Returns
        -------
        identifier : str
        """
        return self._identifier

    def chomp(self, payload):
        """ If the payload starts with the identifier, return the remaining block

        Parameters
        ----------
        payload : str
            Payload blob
        """
        if payload.startswith(self.identifier):
            return payload[len(self.identifier):]


class json_base64(fxPicker_shared):

    _identifier = '00\n'

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = json.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        x = json.loads(self.chomp(payload))
        return x


class pickle_base64(fxPicker_shared):

    _identifier = '01\n'

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = codecs.encode(pickle.dumps(data), 'base64').decode()
        return self.identifier + x

    def deserialize(self, payload):
        data = pickle.loads(codecs.decode(payload.encode(), 'base64'))
        return data

class code_pickle(fxPicker_shared):

    _identifier = '02\n'

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = codecs.encode(pickle.dumps(data), 'base64').decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = pickle.loads(codecs.decode(chomped.encode(), 'base64'))
        return data

class code_text_dill(fxPicker_shared):
    """ We use dill to get the source code out of the function object
    and then exec the function body to load it in. The function object
    is then returned by name.
    """

    _identifier = '03\n'

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        name = data.__name__
        body = dill.source.getsource(data)
        x = codecs.encode(pickle.dumps((name, body)), 'base64').decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        name, body = pickle.loads(codecs.decode(chomped.encode(), 'base64'))
        exec(body)
        return locals()[name]

class code_text_inspect(fxPicker_shared):
    """ We use dill to get the source code out of the function object
    and then exec the function body to load it in. The function object
    is then returned by name.
    """

    _identifier = '03\n'

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        name = data.__name__
        body = inspect.getsource(data)
        x = codecs.encode(pickle.dumps((name, body)), 'base64').decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        name, body = pickle.loads(codecs.decode(chomped.encode(), 'base64'))
        exec(body)
        return locals()[name]


def bar(x, y={'a':3}):
    return x * y['a']

if __name__ == '__main__' :

    def foo(x, y={'a':3}):
        return x * y['a']


    foo(29)
    #print(json_base64.identifier)
    #print(pickle_base64.identifier)
    ct = code_text_inspect()
    f = ct.serialize(foo)
    # print("Serialized : ", f)
    new_foo = ct.deserialize(f)
    print("After deserialization : ", new_foo)
    print("FN() : ", new_foo(10))
