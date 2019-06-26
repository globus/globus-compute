import globus_sdk
import parsl
import os

from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import LocalProvider, KubernetesProvider
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


def _get_executor(container):
    """
    Get the Parsl executor from the container
    
    Returns
    -----------
    executor
    """

    executor = HighThroughputExecutor(
                   label=str(container['container_id']),
                   cores_per_worker=1,
                   max_workers=1,
                   poll_period=10,
                   # launch_cmd="ls; sleep 3600",
                   worker_logdir_root='runinfo',
                   # worker_debug=True,
                   address='140.221.68.108',
                   provider=KubernetesProvider(
                       namespace="dlhub-privileged",
                       image=container['location'],
                       nodes_per_block=1,
                       init_blocks=1,
                       max_blocks=1,
                       parallelism=1,
                       worker_init="""pip install git+https://github.com/Parsl/parsl;
                                   export PYTHONPATH=$PYTHONPATH:/app""",
                       #security=None,
                       secret="ryan-kube-secret",
                       #pod_name=container['container_id'].replace('.', '-').replace("_", '-').replace('/', '-').lower(),
                       #secret="minikube-aws-ecr",
                       #user_id=32781,
                       #group_id=10253,
                       #run_as_non_root=True
                   ),
               )
    return [executor]
