from abc import ABC
from enum import Enum

# IMPORTANT: new error codes can be added, but existing error codes must not be changed once published.
# changing existing error codes will cause problems with users that have older SDK versions
class ResponseErrorCode(int, Enum):
    UNKNOWN_ERROR = 0
    USER_NOT_FOUND = 1
    FUNCTION_NOT_FOUND = 2
    ENDPOINT_NOT_FOUND = 3
    FUNCTION_NOT_PERMITTED = 4
    FORWARDER_REGISTRATION_ERROR = 5
    REQUEST_KEY_ERROR = 6


class FuncxResponseError(Exception, ABC):
    """ Base class for all web service response exceptions
    """
    @property
    def code(self):
        raise NotImplementedError()

    def __init__(self, reason):
        self.error_args = [reason]
        self.reason = reason

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return self.reason
    
    def pack(self):
        return {'status': 'Failed',
                'code': int(self.code),
                'error_args': self.error_args,
                'reason': self.reason}
    
    @classmethod
    def unpack(cls, res_data):
        if 'status' in res_data and res_data['status'] == 'Failed':
            if 'code' in res_data and res_data['code'] != 0 and 'error_args' in res_data:
                try:
                    # if the response error code is not recognized here because the
                    # user is not using the latest SDK version, an exception will occur here
                    # which we will pass in order to give the user a generic exception below
                    res_error_code = ResponseErrorCode(res_data['code'])
                    error_class = None
                    if res_error_code is ResponseErrorCode.USER_NOT_FOUND:
                        error_class = UserNotFound
                    elif res_error_code is ResponseErrorCode.FUNCTION_NOT_FOUND:
                        error_class = FunctionNotFound
                    elif res_error_code is ResponseErrorCode.ENDPOINT_NOT_FOUND:
                        error_class = EndpointNotFound
                    elif res_error_code is ResponseErrorCode.FUNCTION_NOT_PERMITTED:
                        error_class = FunctionNotPermitted
                    elif res_error_code is ResponseErrorCode.FORWARDER_REGISTRATION_ERROR:
                        error_class = ForwarderRegistrationError
                    elif res_error_code is ResponseErrorCode.REQUEST_KEY_ERROR:
                        error_class = RequestKeyError

                    if error_class is not None:
                        return error_class(*res_data['error_args'])
                except Exception:
                    pass
            
            # this is useful for older SDK versions to be compatible with a newer web
            # service: if the SDK does not recognize an error code, it creates a generic
            # exception with the human-readable error reason that was sent
            if 'reason' in res_data:
                return Exception(f"The web service responded with a failure - {res_data['reason']}")
            
            return Exception("The web service failed for an unknown reason")
        
        return None


class UserNotFound(FuncxResponseError):
    """ User not found exception
    """
    code = ResponseErrorCode.USER_NOT_FOUND

    def __init__(self, reason):
        self.error_args = [reason]
        self.reason = reason


class FunctionNotFound(FuncxResponseError):
    """ Function could not be resolved from the database
    """
    code = ResponseErrorCode.FUNCTION_NOT_FOUND

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = "Function {} could not be resolved".format(uuid)
        self.uuid = uuid


class EndpointNotFound(FuncxResponseError):
    """ Endpoint could not be resolved from the database
    """
    code = ResponseErrorCode.ENDPOINT_NOT_FOUND

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = "Endpoint {} could not be resolved".format(uuid)
        self.uuid = uuid


class FunctionNotPermitted(FuncxResponseError):
    """ Function not permitted on endpoint
    """
    code = ResponseErrorCode.FUNCTION_NOT_PERMITTED

    def __init__(self, function_uuid, endpoint_uuid):
        self.error_args = [function_uuid, endpoint_uuid]
        self.reason = "Function {} not permitted on endpoint {}".format(function_uuid, endpoint_uuid)
        self.function_uuid = function_uuid
        self.endpoint_uuid = endpoint_uuid


class ForwarderRegistrationError(FuncxResponseError):
    """ Registering the endpoint with the forwarder has failed
    """
    code = ResponseErrorCode.FORWARDER_REGISTRATION_ERROR

    def __init__(self, forwarder_reason):
        self.error_args = [forwarder_reason]
        self.reason = "Endpoint registration with forwarder failed - {}".format(forwarder_reason)


class RequestKeyError(FuncxResponseError):
    """ User request JSON KeyError exception
    """
    code = ResponseErrorCode.REQUEST_KEY_ERROR

    def __init__(self, key_error_reason):
        self.error_args = [key_error_reason]
        self.reason = "Missing key in JSON request - {}".format(key_error_reason)
