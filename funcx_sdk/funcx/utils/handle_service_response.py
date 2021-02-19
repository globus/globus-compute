from funcx.utils.response_errors import FuncxResponseError


def handle_response_errors(res_data):
    error = FuncxResponseError.unpack(res_data)
    if error is not None:
        raise error

    return
