from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.addresses import address_by_interface

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'perlmutter': {
        'worker_init': 'source ~/setup_funcx_test_env.sh',
        'scheduler_options': '#SBATCH -C gpu'
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            label='Cori_HTEX_multinode',
            address=address_by_interface('bond0.144'),
            provider=SlurmProvider(
                'GPU',  # Partition / QOS
                nodes_per_block=2,
                init_blocks=1,
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler eg: '#SBATCH --constraint=gpu'
                scheduler_options=user_opts['perlmutter']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['perlmutter']['worker_init'],

                # We request all hyperthreads on a node.
                launcher=SrunLauncher(overrides='-c 272'),
                walltime='00:10:00',
                # Slurm scheduler on Cori can be slow at times,
                # increase the command timeouts
                cmd_timeout=120,
            ),
        ),
    ],
)
