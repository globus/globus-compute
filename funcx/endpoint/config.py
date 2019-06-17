import globus_sdk
import parsl
import os

from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor

# GlobusAuth-related secrets
SECRET_KEY = os.environ.get('secret_key')
GLOBUS_KEY = os.environ.get('globus_key')
GLOBUS_CLIENT = os.environ.get('globus_client')

FUNCX_URL = "https://funcx.org/"


def _load_auth_client():
    """Create an AuthClient for the portal

    No credentials are used if the server is not production

    Returns
    -------
    globus_sdk.ConfidentialAppAuthClient
        Client used to perform GlobusAuth actions
    """

    _prod = True

    if _prod:
        app = globus_sdk.ConfidentialAppAuthClient(GLOBUS_CLIENT,
                                                   GLOBUS_KEY)
    else:
        app = globus_sdk.ConfidentialAppAuthClient('', '')
    return app


def _get_parsl_config():
    """Get the Parsl config.

    Returns
    -------
    parsl.config.Config
        Parsl config to execute tasks.
    """

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                worker_debug=False,
                poll_period=1,
                cores_per_worker=1,
                max_workers=1,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                    min_blocks=1,
                ),
            )
        ],
        strategy=None
    )
    return config
