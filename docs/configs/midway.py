from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.addresses import address_by_hostname

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'midway': {
        'worker_init': 'source ~/setup_funcx_test_env.sh',
        'scheduler_options': '',
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=10,
            address=address_by_hostname(),
            provider=SlurmProvider(
                'broadwl',
                launcher=SrunLauncher(),
                nodes_per_block=2,
                init_blocks=1,
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler eg: '#SBATCH --constraint=knl,quad,cache'
                scheduler_options=user_opts['midway']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['midway']['worker_init'],

                min_blocks=1,
                max_blocks=1,
                walltime='00:20:00'
            ),
        )
    ],
    scaling_enabled=True
)
