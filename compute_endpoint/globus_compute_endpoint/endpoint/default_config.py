from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

config = Config(
    display_name=None,  # If None, defaults to the endpoint name
    executors=[
        HighThroughputExecutor(
            provider=LocalProvider(
                init_blocks=1,
                min_blocks=0,
                max_blocks=1,
            ),
        )
    ],
)

# For now, visible_to must be a list of URNs for globus auth users or groups, e.g.:
# urn:globus:auth:identity:{user_uuid}
# urn:globus:groups:id:{group_uuid}
meta = {
    "name": "$name",
    "description": "",
    "organization": "",
    "department": "",
    "public": False,
    "visible_to": [],
}
