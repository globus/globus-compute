from globus_compute_endpoint.strategies.base import BaseStrategy
from globus_compute_endpoint.strategies.kube_simple import KubeSimpleStrategy
from globus_compute_endpoint.strategies.simple import SimpleStrategy

__all__ = ["BaseStrategy", "SimpleStrategy", "KubeSimpleStrategy"]
