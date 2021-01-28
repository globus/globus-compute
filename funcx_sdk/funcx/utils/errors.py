class FuncxError(Exception):
    """ Base class for all funcx exceptions
    """

    def __str__(self):
        return self.__repr__()


class RegistrationError(FuncxError):
    """ Registering the endpoint has failed
    """

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return "Endpoint registration failed due to {}".format(self.reason)


class FuncXUnreachable(FuncxError):
    """ FuncX remote service is unreachable
    """

    def __init__(self, address):
        self.address = address

    def __repr__(self):
        return "FuncX remote service is un-reachable at {}".format(self.address)


class MalformedResponse(FuncxError):
    """ FuncX remote service responded with a Malformed Response
    """

    def __init__(self, response):
        self.response = response

    def __repr__(self):
        return "FuncX remote service responded with Malformed Response: {}".format(self.response)


class FailureResponse(FuncxError):
    """ FuncX remote service responded with a failure
    """

    def __init__(self, response):
        self.response = response

    def __repr__(self):
        return "FuncX remote service failed to fulfill request: {}".format(self.response)


class VersionMismatch(FuncxError):
    """Either client and endpoint version mismatch, or version cannot be retrieved.
    """

    def __init__(self, version_message):
        self.version_message = version_message

    def __repr__(self):
        return "FuncX Versioning Issue: {}".format(self.version_message)


class SerializationError(FuncxError):
    """Something failed during serialization or deserialization.
    """

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return "Serialization Error during: {}".format(self.message)


class UserCancelledException(FuncxError):
    """User cancelled execution
    """

    def __repr__(self):
        return "Task Exception: User Cancelled Execution"


class InvalidScopeException(FuncxError):
    """Invalid API Scope
    """

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return "Invalid Scope: {}".format(self.message)


class HTTPError(FuncxError):
    """An HTTP Request Failed
    """

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return "HTTP request failed: {}".format(self.message)


class InvalidScopeException(FuncxError):
    """Invalid API Scope
    """

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return "Invalid Scope: {}".format(self.message)
