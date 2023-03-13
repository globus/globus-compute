from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.executors import HighThroughputExecutor
from globus_compute_endpoint.providers.kubernetes.kube import KubernetesProvider
from globus_compute_endpoint.strategies import KubeSimpleStrategy
from parsl.addresses import address_by_route

# fmt: off

# PLEASE UPDATE user_opts BEFORE USE
user_opts = {
    'kube': {
        'worker_init': 'pip install --force-reinstall globus_compute_endpoint>=2.0.0',
        'image': 'python:3.8-buster',
        'namespace': 'default',
    }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            label='Kubernetes_funcX',
            max_workers_per_node=1,
            address=address_by_route(),
            scheduler_mode='hard',
            container_type='docker',
            strategy=KubeSimpleStrategy(max_idletime=3600),
            provider=KubernetesProvider(
                init_blocks=0,
                min_blocks=0,
                max_blocks=2,
                init_cpu=1,
                max_cpu=4,
                init_mem="1024Mi",
                max_mem="4096Mi",
                image=user_opts['kube']['image'],
                worker_init=user_opts['kube']['worker_init'],
                namespace=user_opts['kube']['namespace'],
                incluster_config=False,
            ),
        )
    ],
    heartbeat_period=15,
    heartbeat_threshold=200,
    log_dir='.',
)

# fmt: on
