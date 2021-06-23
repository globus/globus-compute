from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.channels import LocalChannel
from parsl.launchers import SrunLauncher
from parsl.providers import LocalProvider, SlurmProvider

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
    executors=[HighThroughputExecutor(
        label="fe.cs.uchicago",
        address=address_by_hostname(),
        provider=SlurmProvider(
            channel=LocalChannel(),
            nodes_per_block=NODES_PER_JOB,
            init_blocks=1,
            partition='general',
            # Launch 4 managers per node, each bound to 1 GPU
            # This is a hack. We use hostname ; to terminate the srun command, and start our own
            # DO NOT MODIFY unless you know what you are doing.
            launcher=SrunLauncher(overrides=(f'hostname; srun --ntasks={TOTAL_WORKERS} '
                                             f'--ntasks-per-node={WORKERS_PER_NODE} '
                                             f'--gpus-per-task=rtx2080ti:{GPUS_PER_WORKER} '
                                             f'--gpu-bind=map_gpu:{GPU_MAP}')
            ),
            walltime='01:00:00',
        ),
    )],
)

# fmt: on
