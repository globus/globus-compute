from parsl.launchers import SingleNodeLauncher
from parsl.providers import PBSProProvider

from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor
from funcx_endpoint.strategies import SimpleStrategy

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'polaris': {
        # Node setup: activate necessary conda environment and such.
        'worker_init': '',
        # PBS directives (header lines): for array jobs pass '-J' option
        # Set ncpus=32, otherwise it defaults to 1 on Polaris
        'scheduler_options': '',
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=1,
            strategy=SimpleStrategy(max_idletime=300),
            # IP of Polaris testbed login node
            address='10.230.2.72',
            provider=PBSProProvider(
                launcher=SingleNodeLauncher(),
                queue='workq',
                scheduler_options=user_opts['polaris']['scheduler_options'],
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['polaris']['worker_init'],
                cpus_per_node=32,
                walltime='01:00:00',
                nodes_per_block=1,
                init_blocks=0,
                min_blocks=0,
                max_blocks=1,
            ),
        )
    ],
)

# fmt: on
