from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider

# fmt: off


# PLEASE CONFIGURE THESE OPTIONS BEFORE USE
NODES_PER_JOB = 2
GPUS_PER_NODE = 4
GPUS_PER_WORKER = 2

# Do not modify:
TOTAL_WORKERS = int((NODES_PER_JOB * GPUS_PER_NODE) / GPUS_PER_WORKER)
WORKERS_PER_NODE = int(GPUS_PER_NODE / GPUS_PER_WORKER)
GPU_MAP = ','.join([str(x) for x in range(1, TOTAL_WORKERS + 1)])

config = Config(
    executors=[
        HighThroughputExecutor(
            label="fe.cs.uchicago",
            worker_debug=False,
            address=address_by_interface('ens2f1'),
            provider=SlurmProvider(
                partition='general',

                # Launch 4 managers per node, each bound to 1 GPU
                # This is a hack. We use hostname ; to terminate the srun command, and
                # start our own
                #
                # DO NOT MODIFY unless you know what you are doing.
                launcher=SrunLauncher(
                    overrides=(
                        f'hostname; srun --ntasks={TOTAL_WORKERS} '
                        f'--ntasks-per-node={WORKERS_PER_NODE} '
                        f'--gpus-per-task=rtx2080ti:{GPUS_PER_WORKER} '
                        f'--gpu-bind=map_gpu:{GPU_MAP}'
                    )
                ),

                # Scale between 0-1 blocks with 2 nodes per block
                nodes_per_block=NODES_PER_JOB,
                init_blocks=0,
                min_blocks=0,
                max_blocks=1,

                # Hold blocks for 30 minutes
                walltime='00:30:00',
            ),
        )],
)

# fmt: on
