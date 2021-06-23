from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider

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
            label="frontera_htex",
            max_workers_per_node=2,
            address=address_by_hostname(),
            provider=SlurmProvider(
                cmd_timeout=60,     # Add extra time for slow scheduler responses
                nodes_per_block=2,
                init_blocks=1,
                min_blocks=0,
                max_blocks=1,
                partition=user_opts['frontera']['partition'],  # Replace with partition name

                # Enter scheduler_options if needed
                scheduler_options=user_opts['frontera']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['frontera']['worker_init'],

                # Ideally we set the walltime to the longest supported walltime.
                walltime='00:10:00',
                launcher=SrunLauncher(),
            ),
        )
    ],
)

# fmt: on
