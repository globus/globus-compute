import os

from parsl.providers import LocalProvider

from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor

CONDA_ENV = os.environ["WORKER_CONDA_ENV"]
print(f"Using conda env:{CONDA_ENV} for worker_init")

config = Config(
    executors=[
        HighThroughputExecutor(
            provider=LocalProvider(
                init_blocks=1,
                min_blocks=0,
                max_blocks=1,
                # FIX ME: Update conda.sh file to match your paths
                worker_init=(
                    "source ~/anaconda3/etc/profile.d/conda.sh; "
                    f"conda activate {CONDA_ENV}; "
                    "python3 --version"
                ),
            ),
        )
    ],
    funcx_service_address="https://api2.funcx.org/v2",
)

# For now, visible_to must be a list of URNs for globus auth users or groups, e.g.:
# urn:globus:auth:identity:{user_uuid}
# urn:globus:groups:id:{group_uuid}
meta = {
    "name": "0.3.3",
    "description": "",
    "organization": "",
    "department": "",
    "public": False,
    "visible_to": [],
}
