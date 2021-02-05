class FuncxResponseError(Exception):
    """ Base class for all web service response exceptions
    """
    code = 0

    def __init__(self, reason):
        self.error_args = [reason]
        self.reason = reason

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return self.reason


class UserNotFound(FuncxResponseError):
    """ User not found exception
    """
    code = 1

    def __init__(self, reason):
        self.error_args = [reason]
        self.reason = reason


class FunctionNotFound(FuncxResponseError):
    """ Function could not be resolved from the database
    """
    code = 2

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = "Function {} could not be resolved".format(uuid)
        self.uuid = uuid


class EndpointNotFound(FuncxResponseError):
    """ Endpoint could not be resolved from the database
    """
    code = 3

    def __init__(self, uuid):
        self.error_args = [uuid]
        self.reason = "Endpoint {} could not be resolved".format(uuid)
        self.uuid = uuid


class FunctionNotPermitted(FuncxResponseError):
    """ Function not permitted on endpoint
    """
    code = 4

    def __init__(self, function_uuid, endpoint_uuid):
        self.error_args = [function_uuid, endpoint_uuid]
        self.reason = "Function {} not permitted on endpoint {}".format(function_uuid, endpoint_uuid)
        self.function_uuid = function_uuid
        self.endpoint_uuid = endpoint_uuid


class ForwarderRegistrationError(FuncxResponseError):
    """ Registering the endpoint with the forwarder has failed
    """
    code = 5

    def __init__(self, forwarder_reason):
        self.error_args = [forwarder_reason]
        self.reason = "Endpoint registration with forwarder failed - {}".format(forwarder_reason)


class RequestKeyError(FuncxResponseError):
    """ User request JSON KeyError exception
    """
    code = 6

    def __init__(self, key_error_reason):
        self.error_args = [key_error_reason]
        self.reason = "Missing key in JSON request - {}".format(key_error_reason)
