from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'bebop': {
        'worker_init': '',
        'scheduler_options': '',
        'partition': 'bdws',
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            address=address_by_interface('ib0'),
            provider=SlurmProvider(
                partition=user_opts['bebop']['partition'],
                launcher=SrunLauncher(),
                nodes_per_block=1,
                init_blocks=1,
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler eg: '#SBATCH --constraint=knl,quad,cache'
                scheduler_options=user_opts['bebop']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['bebop']['worker_init'],

                min_blocks=0,
                max_blocks=1,
                walltime='00:30:00'
            ),
        )
    ],
)
