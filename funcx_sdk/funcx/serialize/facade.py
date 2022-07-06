import logging

from funcx.serialize.concretes import METHODS_MAP_CODE, METHODS_MAP_DATA

logger = logging.getLogger(__name__)


class FuncXSerializer:
    """Wraps several serializers for one uniform interface"""

    def __init__(self):
        """Instantiate the appropriate classes"""

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
        last_exception = None

        if callable(data):
            stype, methods = "Callable", self.methods_for_code.values()
        else:
            stype, methods = "Data", self.methods_for_data.values()
        err_msg = f"{stype} Serialization Method {{}} failed with: {{}}"

        for method in methods:
            try:
                serialized = method.serialize(data)
                break
            except Exception as e:
                logger.debug(err_msg.format(method, e))
                last_exception = e
                continue

        if serialized is None:
            if not last_exception:
                last_exception = NotImplementedError(
                    f"No {stype} serialization methods found"
                )
            raise last_exception

        return serialized

    def deserialize(self, payload):
        """
        Parameters
        ----------
        payload : str
           Payload object to be deserialized

        """
        header = payload[0 : self.header_size]
        if header in self.methods_for_code:
            result = self.methods_for_code[header].deserialize(payload)
        elif header in self.methods_for_data:
            result = self.methods_for_data[header].deserialize(payload)
        else:
            raise Exception(f"Invalid header: {header} in data payload")

        return result

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
