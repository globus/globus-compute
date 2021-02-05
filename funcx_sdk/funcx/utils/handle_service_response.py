import inspect
import funcx.utils.response_errors as response_errors

def handle_response_errors(res_data):
    if 'status' in res_data and res_data['status'] == 'Failed':
        if 'code' in res_data and res_data['code'] != 0 and 'error_args' in res_data:
            res_error_code = res_data['code']
            error_types = inspect.getmembers(response_errors, inspect.isclass)
            for error_type in error_types:
                error_class = error_type[1]
                if issubclass(error_class, response_errors.FuncxResponseError) and res_error_code == error_class.code:
                    raise error_class(*res_data['error_args'])
        
        if 'reason' in res_data:
            raise Exception(f"The web service responded with a failure - {res_data['reason']}")
        else:
            raise Exception("The web service failed for an unknown reason")

    return
