from .response_errors import FuncxResponseError


def handle_response_errors(res_data):
    """Handle a JSON service response and raise the proper exception if the service
    responded with an error. This helper does nothing if the service did not respond
    with an error, but raises the corresponding FuncxResponseError if the service did
    send back a particular error.

    Parameters
    ----------
    res_data : dict
        The JSON data sent from the service
    """
    error = FuncxResponseError.unpack(res_data)
    if error is not None:
        raise error

    return
