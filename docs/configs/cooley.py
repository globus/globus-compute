from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface
from parsl.launchers import MpiExecLauncher
from parsl.providers import CobaltProvider

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'cooley': {
        'worker_init': 'source ~/setup_compute_test_env.sh',
        'scheduler_options': '',
        # Specify the account/allocation to which jobs should be charged
        'account': '<YOUR_COOLEY_ALLOCATION>'
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=2,
            worker_debug=False,
            address=address_by_interface('ib0'),
            provider=CobaltProvider(
                queue='default',
                account=user_opts['cooley']['account'],
                launcher=MpiExecLauncher(),
                # string to prepend to #COBALT blocks in the submit
                # script to the scheduler eg: '#COBALT -t 50'
                scheduler_options=user_opts['cooley']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate compute_env'.
                worker_init=user_opts['cooley']['worker_init'],

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

# fmt: onrom compute_endpoint.endpoint.utils.config import Config
