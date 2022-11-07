# fmt: off

from parsl.providers import LocalProvider

from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=4,

            # Each worker launched will have env vars set to one of (0..3)
            available_accelerators=4,

            # Alternatively a list of strings may be supplied
            # available_accelerators=['opencl:gpu:1', 'opencl:gpu:2']
            provider=LocalProvider(
                init_blocks=1,
                min_blocks=0,
                max_blocks=1,
            ),
        )
    ],
    funcx_service_address="https://api2.funcx.org/v2",
)

# fmt: on
