# fmt: off

from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=4,

            # `available_accelerators` may be a natural number or a list of strings.
            # If an integer, then each worker launched will have an automatically
            # generated environment variable. In this case, one of 0, 1, 2, or 3.
            # Alternatively, specific strings may be utilized.
            available_accelerators=4,
            # available_accelerators=['opencl:gpu:1', 'opencl:gpu:2'] # alternative

            provider=LocalProvider(
                init_blocks=1,
                min_blocks=0,
                max_blocks=1,
            ),
        )
    ],
)

# fmt: on
