from funcx_endpoint.endpoint.utils.config import Config
from parsl.providers import LocalProvider

config = Config(
    executors=[],
    scaling_enabled=True,
    provider=LocalProvider(
        init_blocks=1,
        min_blocks=1,
        max_blocks=1,
    ),
    funcx_service_address='https://api.funcx.org/v1'
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
    "visible_to": []
}
