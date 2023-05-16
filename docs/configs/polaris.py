from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from globus_compute_endpoint.strategies import SimpleStrategy
from parsl.addresses import address_by_interface
from parsl.launchers import MpiExecLauncher
from parsl.providers import PBSProProvider

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'polaris': {
        # Node setup: activate necessary conda environment and such.
        'worker_init': '',
        'scheduler_options': '#PBS -l filesystems=home:grand:eagle\n#PBS -k doe',
        # ALCF allocation to use
        'account': '',
        # Un-comment to give each worker exclusive access to a single GPU
        # 'available_accelerators': 4,
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            available_accelerators=user_opts['polaris'].get('available_accelerators'),
            strategy=SimpleStrategy(max_idletime=300),
            address=address_by_interface('bond0'),
            provider=PBSProProvider(
                launcher=MpiExecLauncher(
                    bind_cmd="--cpu-bind", overrides="--depth=64 --ppn 1"
                ),  # Ensures 1 manger per node, work on all 64 cores
                account=user_opts['polaris']['account'],
                queue='preemptable',
                cpus_per_node=32,
                select_options='ngpus=4',
                worker_init=user_opts['polaris']['worker_init'],
                scheduler_options=user_opts['polaris']['scheduler_options'],
                walltime='01:00:00',
                nodes_per_block=1,
                init_blocks=0,
                min_blocks=0,
                max_blocks=2,
            ),
        )
    ],
)

# fmt: on
