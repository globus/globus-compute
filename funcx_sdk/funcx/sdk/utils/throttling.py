import json
import time

import globus_sdk


class ThrottlingException(Exception):
    pass


class MaxRequestSizeExceeded(ThrottlingException):
    pass


class MaxRequestsExceeded(ThrottlingException):
    pass


class ThrottledBaseClient(globus_sdk.base.BaseClient):
    """
    Throttled base client allows for two different types of Throttling. Note,
    this is 'polite' client side throttling, to avoid well intentioned users
    from mistakenly harming the funcX service, this does not prevent DOS
    attacks.

    Request Flood Throttling: Restricts the number of requests that can be made
    in a given time period, and raises an exception when that has been exceeded

    Request Size Throttling: Restricts the size of the payload that can be
    sent to the FuncX web service.

    Both of these raise exceptions when the values have been exceeded. Both can
    be caught with ThrottlingException. Throttling can also be turned off with:

    cli.throttling_enabled = False

    """

    # Max requests per second, in a 10 second period
    DEFAULT_MAX_REQUESTS = 20
    # Max size is 5Mb
    DEFAULT_MAX_REQUEST_SIZE = 5 * 2 ** 20

    def __init__(self, *args, **kwargs):
        self.max_request_size = self.DEFAULT_MAX_REQUEST_SIZE
        self.max_requests = self.DEFAULT_MAX_REQUESTS

        self.throttling_enabled = True

        self.timer = time.time()
        self.period = 10
        self.requests = 0
        super().__init__(*args, **kwargs)

    def throttle_max_requests(self):
        # Check the period, and reset it if we have moved past the period
        if self.timer + self.period < time.time():
            self.timer = time.time()
            self.requests = 0

        self.requests += 1

        if self.requests > self.max_requests:
            raise MaxRequestsExceeded()

    def throttle_request_size(self, *request_args, **request_kwargs):
        rtype, path = request_args
        if request_kwargs.get("json_body"):
            size = len(json.dumps(request_kwargs["json_body"]))
        elif request_kwargs.get("text_body"):
            size = len(request_kwargs["text_body"])
        else:
            return
        if rtype == "POST" and size > self.max_request_size:
            raise MaxRequestSizeExceeded(
                f"Size of {rtype} at {path} was {size} bytes, exceeded "
                f"limit of {self.max_request_size}."
            )

    def _request(self, *args, **kwargs):
        if self.throttling_enabled is True:
            self.throttle_request_size(*args, **kwargs)
            self.throttle_max_requests()
        return super()._request(*args, **kwargs)
