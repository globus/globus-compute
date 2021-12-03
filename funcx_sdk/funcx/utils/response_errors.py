from abc import ABC
from enum import Enum


# IMPORTANT: new error codes can be added, but existing error codes must not be changed
# once published.
# changing existing error codes will cause problems with users that have older SDK
# versions
class ResponseErrorCode(int, Enum):
    UNKNOWN_ERROR = 0
    USER_UNAUTHENTICATED = 1
    USER_NOT_FOUND = 2
    FUNCTION_NOT_FOUND = 3
    ENDPOINT_NOT_FOUND = 4
    CONTAINER_NOT_FOUND = 5
    TASK_NOT_FOUND = 6
    AUTH_GROUP_NOT_FOUND = 7
    FUNCTION_ACCESS_FORBIDDEN = 8
    ENDPOINT_ACCESS_FORBIDDEN = 9
    FUNCTION_NOT_PERMITTED = 10
    ENDPOINT_ALREADY_REGISTERED = 11
    FORWARDER_REGISTRATION_ERROR = 12
    FORWARDER_CONTACT_ERROR = 13
    ENDPOINT_STATS_ERROR = 14
    LIVENESS_STATS_ERROR = 15
    REQUEST_KEY_ERROR = 16
    REQUEST_MALFORMED = 17
    INTERNAL_ERROR = 18
    ENDPOINT_OUTDATED = 19
    TASK_GROUP_NOT_FOUND = 20
    TASK_GROUP_ACCESS_FORBIDDEN = 21
    INVALID_UUID = 22


# a collection of the HTTP status error codes that the service would make use of
class HTTPStatusCode(int, Enum):
    BAD_REQUEST = 400
    # semantically this response means "unauthenticated", according to the spec
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    REQUEST_TIMEOUT = 408
    TOO_MANY_REQUESTS = 429
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504


class FuncxResponseError(Exception, ABC):
    """Base class for all web service response exceptions"""

    @property
    def code(self):
        raise NotImplementedError()

    @property
    def http_status_code(self):
        raise NotImplementedError()

    def __init__(self, reason):
        self.error_args = [reason]
        self.reason = reason

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return self.reason

    def pack(self):
        return {
            "status": "Failed",
            "code": int(self.code),
            "error_args": self.error_args,
            "reason": self.reason,
            "http_status_code": int(self.http_status_code),
        }

    @classmethod
    def unpack(cls, res_data):
        if "status" in res_data and res_data["status"] == "Failed":
            if (
                "code" in res_data
                and res_data["code"] != 0
                and "error_args" in res_data
            ):
                try:
                    # if the response error code is not recognized here because the
                    # user is not using the latest SDK version, an exception will occur
                    # here, which we will pass in order to give the user a generic
                    # exception below
                    res_error_code = ResponseErrorCode(res_data["code"])
                    error_class = None
                    if res_error_code is ResponseErrorCode.USER_UNAUTHENTICATED:
                        error_class = UserUnauthenticated
                    elif res_error_code is ResponseErrorCode.USER_NOT_FOUND:
                        error_class = UserNotFound
                    elif res_error_code is ResponseErrorCode.FUNCTION_NOT_FOUND:
                        error_class = FunctionNotFound
                    elif (
                        res_error_code is ResponseErrorCode.ENDPOINT_ALREADY_REGISTERED
                    ):
                        error_class = EndpointAlreadyRegistered
                    elif res_error_code is ResponseErrorCode.ENDPOINT_NOT_FOUND:
                        error_class = EndpointNotFound
                    elif res_error_code is ResponseErrorCode.CONTAINER_NOT_FOUND:
                        error_class = ContainerNotFound
                    elif res_error_code is ResponseErrorCode.TASK_NOT_FOUND:
                        error_class = TaskNotFound
                    elif res_error_code is ResponseErrorCode.AUTH_GROUP_NOT_FOUND:
                        error_class = AuthGroupNotFound
                    elif res_error_code is ResponseErrorCode.FUNCTION_ACCESS_FORBIDDEN:
                        error_class = FunctionAccessForbidden
                    elif res_error_code is ResponseErrorCode.ENDPOINT_ACCESS_FORBIDDEN:
                        error_class = EndpointAccessForbidden
                    elif res_error_code is ResponseErrorCode.FUNCTION_NOT_PERMITTED:
                        error_class = FunctionNotPermitted
                    elif (
                        res_error_code is ResponseErrorCode.FORWARDER_REGISTRATION_ERROR
                    ):
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
                    elif res_error_code is ResponseErrorCode.INTERNAL_ERROR:
                        error_class = InternalError
                    elif res_error_code is ResponseErrorCode.ENDPOINT_OUTDATED:
                        error_class = EndpointOutdated
                    elif res_error_code is ResponseErrorCode.TASK_GROUP_NOT_FOUND:
                        error_class = TaskGroupNotFound
                    elif (
                        res_error_code is ResponseErrorCode.TASK_GROUP_ACCESS_FORBIDDEN
                    ):
                        error_class = TaskGroupAccessForbidden
                    elif res_error_code is ResponseErrorCode.INVALID_UUID:
                        error_class = InvalidUUID

                    if error_class is not None:
                        return error_class(*res_data["error_args"])
                except Exception:
                    pass

            # this is useful for older SDK versions to be compatible with a newer web
            # service: if the SDK does not recognize an error code, it creates a generic
            # exception with the human-readable error reason that was sent
            if "reason" in res_data:
                return Exception(
                    f"The web service responded with a failure - {res_data['reason']}"
                )

            return Exception("The web service failed for an unknown reason")

        return None


class UserUnauthenticated(FuncxResponseError):
    """User unauthenticated. This differs from UserNotFound in that it has a 401 HTTP
    status code, whereas UserNotFound has a 404 status code. This error should be used
    when the user's request failed because the user was unauthenticated, regardless of
    whether the request itself required looking up user info.
    """

    code = ResponseErrorCode.USER_UNAUTHENTICATED
    # this HTTP status code is called unauthorized but really means "unauthenticated"
    # according to the spec
    http_status_code = HTTPStatusCode.UNAUTHORIZED

    def __init__(self):
        self.error_args = []
        self.reason = (
            "Could not find user. You must be logged in to perform this function."
        )


class UserNotFound(FuncxResponseError):
    """
    User not found exception. This error should only be used when the server must look
    up a user in order to fulfill the user's request body. If the request only fails
    because the user is unauthenticated, UserUnauthenticated should be used instead.
    """

    code = ResponseErrorCode.USER_NOT_FOUND
    http_status_code = HTTPStatusCode.NOT_FOUND

    def __init__(self, reason):
        reason = str(reason)
        self.error_args = [reason]
        self.reason = reason


class FunctionNotFound(FuncxResponseError):
    """Function could not be resolved from the database"""

    code = ResponseErrorCode.FUNCTION_NOT_FOUND
    http_status_code = HTTPStatusCode.NOT_FOUND

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Function {uuid} could not be resolved"
        self.uuid = uuid


class EndpointNotFound(FuncxResponseError):
    """Endpoint could not be resolved from the database"""

    code = ResponseErrorCode.ENDPOINT_NOT_FOUND
    http_status_code = HTTPStatusCode.NOT_FOUND

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Endpoint {uuid} could not be resolved"
        self.uuid = uuid


class ContainerNotFound(FuncxResponseError):
    """Container could not be resolved"""

    code = ResponseErrorCode.CONTAINER_NOT_FOUND
    http_status_code = HTTPStatusCode.NOT_FOUND

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Container {uuid} not found"
        self.uuid = uuid


class TaskNotFound(FuncxResponseError):
    """Task could not be resolved"""

    code = ResponseErrorCode.TASK_NOT_FOUND
    http_status_code = HTTPStatusCode.NOT_FOUND

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Task {uuid} not found"
        self.uuid = uuid


class AuthGroupNotFound(FuncxResponseError):
    """AuthGroup could not be resolved"""

    code = ResponseErrorCode.AUTH_GROUP_NOT_FOUND
    http_status_code = HTTPStatusCode.NOT_FOUND

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"AuthGroup {uuid} not found"
        self.uuid = uuid


class FunctionAccessForbidden(FuncxResponseError):
    """Unauthorized function access by user"""

    code = ResponseErrorCode.FUNCTION_ACCESS_FORBIDDEN
    http_status_code = HTTPStatusCode.FORBIDDEN

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Unauthorized access to function {uuid}"
        self.uuid = uuid


class EndpointAccessForbidden(FuncxResponseError):
    """Unauthorized endpoint access by user"""

    code = ResponseErrorCode.ENDPOINT_ACCESS_FORBIDDEN
    http_status_code = HTTPStatusCode.FORBIDDEN

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Unauthorized access to endpoint {uuid}"
        self.uuid = uuid


class FunctionNotPermitted(FuncxResponseError):
    """Function not permitted on endpoint"""

    code = ResponseErrorCode.FUNCTION_NOT_PERMITTED
    http_status_code = HTTPStatusCode.FORBIDDEN

    def __init__(self, function_uuid, endpoint_uuid):
        self.error_args = [function_uuid, endpoint_uuid]
        self.reason = (
            f"Function {function_uuid} not permitted on endpoint {endpoint_uuid}"
        )
        self.function_uuid = function_uuid
        self.endpoint_uuid = endpoint_uuid


class EndpointAlreadyRegistered(FuncxResponseError):
    """Endpoint with specified uuid already registered by a different user"""

    code = ResponseErrorCode.ENDPOINT_ALREADY_REGISTERED
    http_status_code = HTTPStatusCode.BAD_REQUEST

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Endpoint {uuid} was already registered by a different user"
        self.uuid = uuid


class ForwarderRegistrationError(FuncxResponseError):
    """Registering the endpoint with the forwarder has failed"""

    code = ResponseErrorCode.FORWARDER_REGISTRATION_ERROR
    http_status_code = HTTPStatusCode.BAD_GATEWAY

    def __init__(self, error_reason):
        error_reason = str(error_reason)
        self.error_args = [error_reason]
        self.reason = f"Endpoint registration with forwarder failed - {error_reason}"


class ForwarderContactError(FuncxResponseError):
    """Contacting the forwarder failed"""

    code = ResponseErrorCode.FORWARDER_CONTACT_ERROR
    http_status_code = HTTPStatusCode.BAD_GATEWAY

    def __init__(self, error_reason):
        error_reason = str(error_reason)
        self.error_args = [error_reason]
        self.reason = f"Contacting forwarder failed with {error_reason}"


class EndpointStatsError(FuncxResponseError):
    """Error while retrieving endpoint stats"""

    code = ResponseErrorCode.ENDPOINT_STATS_ERROR
    http_status_code = HTTPStatusCode.INTERNAL_SERVER_ERROR

    def __init__(self, endpoint_uuid, error_reason):
        error_reason = str(error_reason)
        self.error_args = [endpoint_uuid, error_reason]
        self.reason = (
            f"Unable to retrieve stats for endpoint: {endpoint_uuid}. {error_reason}"
        )


class LivenessStatsError(FuncxResponseError):
    """Error while retrieving endpoint stats"""

    code = ResponseErrorCode.LIVENESS_STATS_ERROR
    http_status_code = HTTPStatusCode.BAD_GATEWAY

    def __init__(self, http_status_code):
        self.error_args = [http_status_code]
        self.reason = "Forwarder did not respond with liveness stats"


class RequestKeyError(FuncxResponseError):
    """User request JSON KeyError exception"""

    code = ResponseErrorCode.REQUEST_KEY_ERROR
    http_status_code = HTTPStatusCode.BAD_REQUEST

    def __init__(self, key_error_reason):
        key_error_reason = str(key_error_reason)
        self.error_args = [key_error_reason]
        self.reason = f"Missing key in JSON request - {key_error_reason}"


class RequestMalformed(FuncxResponseError):
    """User request malformed"""

    code = ResponseErrorCode.REQUEST_MALFORMED
    http_status_code = HTTPStatusCode.BAD_REQUEST

    def __init__(self, malformed_reason):
        malformed_reason = str(malformed_reason)
        self.error_args = [malformed_reason]
        self.reason = (
            f"Request Malformed. Missing critical information: {malformed_reason}"
        )


class InternalError(FuncxResponseError):
    """Internal server error"""

    code = ResponseErrorCode.INTERNAL_ERROR
    http_status_code = HTTPStatusCode.INTERNAL_SERVER_ERROR

    def __init__(self, error_reason):
        error_reason = str(error_reason)
        self.error_args = [error_reason]
        self.reason = f"Internal server error: {error_reason}"


class EndpointOutdated(FuncxResponseError):
    """Internal server error"""

    code = ResponseErrorCode.ENDPOINT_OUTDATED
    http_status_code = HTTPStatusCode.BAD_REQUEST

    def __init__(self, min_ep_version):
        self.error_args = [min_ep_version]
        self.reason = (
            "Endpoint is out of date. "
            f"Minimum supported endpoint version is {min_ep_version}"
        )


class TaskGroupNotFound(FuncxResponseError):
    """Task Group was not found in redis"""

    code = ResponseErrorCode.TASK_GROUP_NOT_FOUND
    http_status_code = HTTPStatusCode.NOT_FOUND

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Task Group {uuid} could not be resolved"
        self.uuid = uuid


class TaskGroupAccessForbidden(FuncxResponseError):
    """Unauthorized Task Group access by user"""

    code = ResponseErrorCode.TASK_GROUP_ACCESS_FORBIDDEN
    http_status_code = HTTPStatusCode.FORBIDDEN

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = f"Unauthorized access to Task Group {uuid}"
        self.uuid = uuid


class InvalidUUID(FuncxResponseError):
    """Invalid UUID provided by user"""

    code = ResponseErrorCode.INVALID_UUID
    http_status_code = HTTPStatusCode.BAD_REQUEST

    def __init__(self, reason):
        reason = str(reason)
        self.error_args = [reason]
        self.reason = reason
