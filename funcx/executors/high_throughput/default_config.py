from funcx.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel

# Create a config object
config = HighThroughputExecutor(
    provider=LocalProvider(
        channel=LocalChannel
    )
)
