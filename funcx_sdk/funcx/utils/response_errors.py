from abc import ABC
from enum import Enum

# IMPORTANT: new error codes can be added, but existing error codes must not be changed once published.
# changing existing error codes will cause problems with users that have older SDK versions
class ResponseErrorCode(int, Enum):
    UNKNOWN_ERROR = 0
    USER_NOT_FOUND = 1
    FUNCTION_NOT_FOUND = 2
    ENDPOINT_NOT_FOUND = 3
    UNAUTHORIZED_FUNCTION_ACCESS = 4
    UNAUTHORIZED_ENDPOINT_ACCESS = 5
    FUNCTION_NOT_PERMITTED = 6
    FORWARDER_REGISTRATION_ERROR = 7
    FORWARDER_CONTACT_ERROR = 8
    ENDPOINT_STATS_ERROR = 9
    LIVENESS_STATS_ERROR = 10
    REQUEST_KEY_ERROR = 11
    REQUEST_MALFORMED = 12


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
                    elif res_error_code is ResponseErrorCode.UNAUTHORIZED_FUNCTION_ACCESS:
                        error_class = UnauthorizedFunctionAccess
                    elif res_error_code is ResponseErrorCode.UNAUTHORIZED_ENDPOINT_ACCESS:
                        error_class = UnauthorizedEndpointAccess
                    elif res_error_code is ResponseErrorCode.FUNCTION_NOT_PERMITTED:
                        error_class = FunctionNotPermitted
                    elif res_error_code is ResponseErrorCode.FORWARDER_REGISTRATION_ERROR:
                        error_class = ForwarderRegistrationError
                    elif res_error_code is ResponseErrorCode.FORWARDER_CONTACT_ERROR:
                        error_class = ForwarderContactError
                    elif res_error_code is ResponseErrorCode.ENDPOINT_STATS_ERROR:
                        error_class = EndpointStatsError
                    elif res_error_code is ResponseErrorCode.LIVENESS_STATS_ERROR:
                        error_class = LivenessStatsError
                    elif res_error_code is ResponseErrorCode.REQUEST_KEY_ERROR:
                        error_class = RequestKeyError
                    elif res_error_code is ResponseErrorCode.REQUEST_MALFORMED:
                        error_class = RequestMalformed

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
        self.reason = f"Function {uuid} could not be resolved"
        self.uuid = uuid


class EndpointNotFound(FuncxResponseError):
    """ Endpoint could not be resolved from the database
    """
    code = ResponseErrorCode.ENDPOINT_NOT_FOUND

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Endpoint {uuid} could not be resolved"
        self.uuid = uuid


class UnauthorizedFunctionAccess(FuncxResponseError):
    """ Unauthorized function access by user
    """
    code = ResponseErrorCode.UNAUTHORIZED_FUNCTION_ACCESS

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Unauthorized access to function {uuid}"
        self.uuid = uuid


class UnauthorizedEndpointAccess(FuncxResponseError):
    """ Unauthorized endpoint access by user
    """
    code = ResponseErrorCode.UNAUTHORIZED_ENDPOINT_ACCESS

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Unauthorized access to endpoint {uuid}"
        self.uuid = uuid


class FunctionNotPermitted(FuncxResponseError):
    """ Function not permitted on endpoint
    """
    code = ResponseErrorCode.FUNCTION_NOT_PERMITTED

    def __init__(self, function_uuid, endpoint_uuid):
        self.error_args = [function_uuid, endpoint_uuid]
        self.reason = f"Function {function_uuid} not permitted on endpoint {endpoint_uuid}"
        self.function_uuid = function_uuid
        self.endpoint_uuid = endpoint_uuid


class ForwarderRegistrationError(FuncxResponseError):
    """ Registering the endpoint with the forwarder has failed
    """
    code = ResponseErrorCode.FORWARDER_REGISTRATION_ERROR

    def __init__(self, error_reason):
        self.error_args = [error_reason]
        self.reason = f"Endpoint registration with forwarder failed - {error_reason}"


class ForwarderContactError(FuncxResponseError):
    """ Contacting the forwarder failed
    """
    code = ResponseErrorCode.FORWARDER_CONTACT_ERROR

    def __init__(self, error_reason):
        self.error_args = [error_reason]
        self.reason = f"Contacting forwarder failed with {error_reason}"


class EndpointStatsError(FuncxResponseError):
    """ Error while retrieving endpoint stats
    """
    code = ResponseErrorCode.ENDPOINT_STATS_ERROR

    def __init__(self, endpoint_uuid, error_reason):
        self.error_args = [endpoint_uuid, error_reason]
        self.reason = f"Unable to retrieve endpoint stats: {endpoint_id}. {e}"


class LivenessStatsError(FuncxResponseError):
    """ Error while retrieving endpoint stats
    """
    code = ResponseErrorCode.LIVENESS_STATS_ERROR

    def __init__(self, http_status_code):
        self.error_args = [http_status_code]
        self.reason = "Forwarder did not respond with liveness stats"


class RequestKeyError(FuncxResponseError):
    """ User request JSON KeyError exception
    """
    code = ResponseErrorCode.REQUEST_KEY_ERROR

    def __init__(self, key_error_reason):
        self.error_args = [key_error_reason]
        self.reason = f"Missing key in JSON request - {key_error_reason}"


class RequestMalformed(FuncxResponseError):
    """ User request malformed
    """
    code = ResponseErrorCode.REQUEST_MALFORMED

    def __init__(self, malformed_reason):
        self.error_args = [malformed_reason]
        self.reason = f"Request Malformed. Missing critical information: {malformed_reason}"
