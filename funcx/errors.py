
class FuncxError(Exception):
    """ Base class for all funcx exceptions
    """

    def __str__(self):
        return self.__repr__


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
