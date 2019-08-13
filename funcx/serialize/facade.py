from funcx.serialize.concretes import *
from funcx.serialize.base import METHODS_MAP_DATA, METHODS_MAP_CODE
import logging

logger = logging.getLogger(__name__)

class FuncXSerializer(object):
    """
    Information that we want to be able to ship around:


    * Function run information ->
    * Function id <---> Container id ? (is there a 1-1 mapping here?)
    * Container id
    * Endpoint id

    * Function body
    * Potentially in a byte compiled form ?
    * All parameters ->
    * Args  + Kwargs


    From the client side. At function invocation we need to capture

    """

    def __init__(self):
        """ Instantiate the appropriate classes
        """

        # Do we want to do a check on header size here ? Probably overkill
        headers = list(METHODS_MAP_CODE.keys()) + list(METHODS_MAP_DATA.keys())
        self.header_size = len(headers[0])

        self.methods_for_code = {}
        self.methods_for_data = {}

        for key in METHODS_MAP_CODE:
            self.methods_for_code[key] = METHODS_MAP_CODE[key]()
        for key in METHODS_MAP_DATA:
            self.methods_for_data[key] = METHODS_MAP_DATA[key]()

    def _list_methods(self):
        return self.methods_for_code, self.methods_for_data


    def serialize(self, data):
        serialized = None

        if callable(data):
            for method in self.methods_for_code.values():
                try:
                    serialized = method.serialize(data)
                except Exception as e:
                    logger.exception("Method {} did not work".format(method))
                    continue
        else:
            for method in self.methods_for_data.values():
                try:
                    serialized = method.serialize(data)
                except Exception as e:
                    logger.exception("Method {} did not work".format(method))
                    continue

        if serialized is None:
            raise Exception("None of serialization methods were able to serialize {}".format(data))
        return serialized

    def deserialize(self, payload):
        """
        Parameters
        ----------
        payload : str
           Payload object to be deserialized

        """
        header = payload[0:self.header_size]
        if header in self.methods_for_code:
            result = self.methods_for_code[header].deserialize(payload)
        elif header in self.methods_for_data:
            result = self.methods_for_data[header].deserialize(payload)
        else:
            raise Exception("Invalid header: {} in data payload".format(header))

        print("Header: ", header)
        print("Result: ", result)
        return result


    def pack_buffers(self, buffers):
        """
        Parameters
        ----------
        buffers : list of \n terminated strings
        """
        packed = ''
        for buf in buffers:
            s_length = str(len(buf)) + '\n'
            packed += s_length + buf

        return packed

    def unpack_buffers(self, packed_buffer):
        """
        Parameters
        ----------
        packed_buffers : packed buffer as string
        """
        unpacked = []
        while packed_buffer:
            s_length, buf = packed_buffer.split('\n', 1)
            i_length = int(s_length)
            current, packed_buffer = buf[:i_length], buf[i_length:]
            unpacked.extend([current])

        return unpacked

    def unpack_and_deserialize(self, packed_buffer):
        """ Unpacks a packed buffer and returns the deserialized contents
        Parameters
        ----------
        packed_buffers : packed buffer as string
        """
        unpacked = []
        while packed_buffer:
            s_length, buf = packed_buffer.split('\n', 1)
            i_length = int(s_length)
            current, packed_buffer = buf[:i_length], buf[i_length:]
            deserialized = self.deserialize(current)
            unpacked.extend([deserialized])

        assert len(unpacked) == 3, "Unpack expects 3 buffers, got {}".format(len(unpacked))

        return unpacked
