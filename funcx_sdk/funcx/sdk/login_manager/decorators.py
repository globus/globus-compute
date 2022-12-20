import functools
import logging

import globus_sdk

log = logging.getLogger(__name__)


def requires_login(func):
    """Decorator that initiates a new auth flow when
    an API auth error is raised.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except globus_sdk.AuthAPIError as e:
            log.debug(
                "Unauthorized API call (callable: %s).  Exception text: %s",
                func.__name__,
                e,
            )
            self.login_manager.run_login_flow()
            # Initiate a new web client with updated authorizer
            self.web_client = self.login_manager.get_funcx_web_client(
                base_url=self.funcx_service_address
            )
            return func(self, *args, **kwargs)

    return wrapper
