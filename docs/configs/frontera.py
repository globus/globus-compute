from parsl.addresses import address_by_hostname
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider

from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'frontera': {
        'worker_init': 'source ~/setup_funcx_test_env.sh',
        'scheduler_options': '#SBATCH -A MCB20024',
        'partition': 'development',

    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=2,
            worker_debug=False,
            address=address_by_hostname(),
            provider=SlurmProvider(
                partition=user_opts['frontera']['partition'],
                launcher=SrunLauncher(),

                # Enter scheduler_options if needed
                scheduler_options=user_opts['frontera']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['frontera']['worker_init'],

                # Add extra time for slow scheduler responses
                cmd_timeout=60,

                # Scale between 0-1 blocks with 2 nodes per block
                nodes_per_block=2,
                init_blocks=0,
                min_blocks=0,
                max_blocks=1,

                # Hold blocks for 30 minutes
                walltime='00:30:00',
            ),
        )
    ],
)

# fmt: on
