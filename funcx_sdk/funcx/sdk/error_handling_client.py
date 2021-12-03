from globus_sdk import GlobusAPIError

from funcx.sdk.utils.throttling import ThrottledBaseClient
from funcx.utils.errors import HTTPError
from funcx.utils.handle_service_response import handle_response_errors


class FuncXErrorHandlingClient(ThrottledBaseClient):
    """
    Class which handles errors from GET, POST, and DELETE requests before proceeding
    """

    def get(self, path, **kwargs):
        try:
            r = ThrottledBaseClient.get(self, path, **kwargs)
        except GlobusAPIError as e:
            # this error has a raw_json property which we can use to create our
            # own custom error
            handle_response_errors(e.raw_json)
            # the above error handler should raise an error, but the GlobusAPIError
            # is a fallback in case there is no raw_json property
            raise e

        # the Globus API should catch this situation already, so this is
        # just here as a fallback (should allow 200 and 207 through)
        if r.http_status >= 400:
            raise HTTPError(r)

        return r

    def post(self, path, **kwargs):
        try:
            r = ThrottledBaseClient.post(self, path, **kwargs)
        except GlobusAPIError as e:
            handle_response_errors(e.raw_json)
            raise e

        if r.http_status >= 400:
            raise HTTPError(r)

        return r

    def delete(self, path, **kwargs):
        try:
            r = ThrottledBaseClient.delete(self, path, **kwargs)
        except GlobusAPIError as e:
            handle_response_errors(e.raw_json)
            raise e

        if r.http_status >= 400:
            raise HTTPError(r)

        return r
