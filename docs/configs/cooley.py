from parsl.addresses import address_by_hostname
from parsl.launchers import MpiExecLauncher
from parsl.providers import CobaltProvider

from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'cooley': {
        'worker_init': 'source ~/setup_funcx_test_env.sh',
        'scheduler_options': '',
        # Specify the account/allocation to which jobs should be charged
        'account': '<YOUR_COOLEY_ALLOCATION>'
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=2,
            address=address_by_hostname(),
            provider=CobaltProvider(
                queue='default',
                account=user_opts['cooley']['account'],
                launcher=MpiExecLauncher(),
                # string to prepend to #COBALT blocks in the submit
                # script to the scheduler eg: '#COBALT -t 50'
                scheduler_options=user_opts['cooley']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate funcx_env'.
                worker_init=user_opts['cooley']['worker_init'],

                walltime='00:30:00',
                nodes_per_block=1,
                init_blocks=0,
                min_blocks=0,
                max_blocks=4,
            ),
        )
    ],
)

# fmt: onrom funcx_endpoint.endpoint.utils.config import Config