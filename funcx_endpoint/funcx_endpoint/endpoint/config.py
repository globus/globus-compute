import os

import globus_sdk
from parsl.addresses import address_by_route
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import KubernetesProvider, LocalProvider

# GlobusAuth-related secrets
SECRET_KEY = os.environ.get("secret_key")
GLOBUS_KEY = os.environ.get("globus_key")
GLOBUS_CLIENT = os.environ.get("globus_client")

FUNCX_URL = "https://funcx.org/"
FUNCX_HUB_URL = "3.88.81.131"


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
        app = globus_sdk.ConfidentialAppAuthClient(GLOBUS_CLIENT, GLOBUS_KEY)
    else:
        app = globus_sdk.ConfidentialAppAuthClient("", "")
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
        strategy=None,
    )
    return config


def _get_executor(container):
    """
    Get the Parsl executor from the container

    Returns
    -----------
    executor
    """

    executor = HighThroughputExecutor(
        label=container["container_uuid"],
        cores_per_worker=1,
        max_workers=1,
        poll_period=10,
        # launch_cmd="ls; sleep 3600",
        worker_logdir_root="runinfo",
        # worker_debug=True,
        address=address_by_route(),
        provider=KubernetesProvider(
            namespace="dlhub-privileged",
            image=container["location"],
            nodes_per_block=1,
            init_blocks=1,
            max_blocks=1,
            parallelism=1,
            worker_init="""pip install git+https://github.com/Parsl/parsl;
                                   pip install git+https://github.com/funcx-faas/funcX;
                                   export PYTHONPATH=$PYTHONPATH:/home/ubuntu:/app""",
            # security=None,
            secret="ryan-kube-secret",
            pod_name=container["name"]
            .replace(".", "-")
            .replace("_", "-")
            .replace("/", "-")
            .lower(),
            # secret="minikube-aws-ecr",
            # user_id=32781,
            # group_id=10253,
            # run_as_non_root=True
        ),
    )
    return [executor]
