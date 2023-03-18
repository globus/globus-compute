from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.launchers import AprunLauncher
from parsl.providers import TorqueProvider

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'bluewaters': {
        'worker_init': 'module load bwpy;source anaconda3/etc/profile.d/conda.sh;conda activate funcx_testing_py3.7',  # noqa: E501
        'scheduler_options': '',
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            max_workers_per_node=1,
            worker_debug=False,
            address=address_by_hostname(),
            provider=TorqueProvider(
                queue='normal',
                launcher=AprunLauncher(overrides="-b -- bwpy-environ --"),

                # string to prepend to #SBATCH blocks in the submit
                scheduler_options=user_opts['bluewaters']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load bwpy; source activate compute env'.
                worker_init=user_opts['bluewaters']['worker_init'],

                # Scale between 0-1 blocks with 2 nodes per block
                nodes_per_block=2,
                init_blocks=0,
                min_blocks=0,
                max_blocks=1,

                # Hold blocks for 30 minutes
                walltime='00:30:00'
            ),
        )

    ],
)

# fmt: on
