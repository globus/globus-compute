from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'perlmutter': {
        'worker_init': 'source ~/setup_compute_test_env.sh',
        'scheduler_options': '#SBATCH -C gpu'
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            worker_debug=False,
            address=address_by_interface('nmn0'),
            provider=SlurmProvider(
                partition='GPU',  # Partition / QOS

                # We request all hyperthreads on a node.
                launcher=SrunLauncher(overrides='-c 272'),

                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler eg: '#SBATCH --constraint=gpu'
                scheduler_options=user_opts['perlmutter']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['perlmutter']['worker_init'],

                # Slurm scheduler on Cori can be slow at times,
                # increase the command timeouts
                cmd_timeout=120,

                # Scale between 0-1 blocks with 2 nodes per block
                nodes_per_block=2,
                init_blocks=0,
                min_blocks=0,
                max_blocks=1,

                # Hold blocks for 10 minutes
                walltime='00:10:00',
            ),
        ),
    ],
)

# fmt: on
