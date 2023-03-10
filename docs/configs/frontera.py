from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'frontera': {
        'worker_init': 'source ~/setup_compute_test_env.sh',
        'account': 'EAR22001',
        'partition': 'development',
        'scheduler_options': '',
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=2,
            worker_debug=False,
            address=address_by_interface('ib0'),
            provider=SlurmProvider(
                account=user_opts['frontera']['account'],
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
