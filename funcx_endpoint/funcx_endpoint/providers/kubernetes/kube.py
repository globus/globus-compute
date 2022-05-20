import logging
import queue
import time
from typing import Any, Dict, List, Optional, Tuple

import typeguard
from parsl.errors import OptionalModuleMissing
from parsl.providers.provider_base import ExecutionProvider
from parsl.utils import RepresentationMixin

from funcx_endpoint.providers.kubernetes.template import template_string

try:
    from kubernetes import client, config

    _kubernetes_enabled = True
except (ImportError, NameError, FileNotFoundError):
    _kubernetes_enabled = False

log = logging.getLogger(__name__)


class KubernetesProvider(ExecutionProvider, RepresentationMixin):
    """Kubernetes execution provider
    Parameters
    ----------
    namespace : str
        Kubernetes namespace to create deployments.
    image : str
        Docker image to use in the deployment.
    nodes_per_block : int
        Nodes to provision per block.
    init_blocks : int
        Number of blocks to provision at the start of the run. Default is 0.
    min_blocks : int
        Minimum number of blocks to maintain.
    max_blocks : int
        Maximum number of blocks to maintain.
    max_cpu : float
        CPU limits of the blocks (pods), in cpu units.
        This is the cpu "limits" option for resource specification.
        Check kubernetes docs for more details. Default is 2.
    max_mem : str
        Memory limits of the blocks (pods), in Mi or Gi.
        This is the memory "limits" option for resource specification on kubernetes.
        Check kubernetes docs for more details. Default is 500Mi.
    init_cpu : float
        CPU limits of the blocks (pods), in cpu units.
        This is the cpu "requests" option for resource specification.
        Check kubernetes docs for more details. Default is 1.
    init_mem : str
        Memory limits of the blocks (pods), in Mi or Gi.
        This is the memory "requests" option for resource specification on kubernetes.
        Check kubernetes docs for more details. Default is 250Mi.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of 1
        represents aggressive scaling where as many resources as possible are used;
        parallelism close to 0 represents the opposite situation in which as few
        resources as possible (i.e., min_blocks) are used.
    worker_init : str
        Command to be run first for the workers, such as `python start.py`.
    secret : str
        Docker secret to use to pull images
    pod_name : str
        The name for the pod, will be appended with a timestamp.
        Default is None, meaning parsl automatically names the pod.
    user_id : str
        Unix user id to run the container as.
    group_id : str
        Unix group id to run the container as.
    run_as_non_root : bool
        Run as non-root (True) or run as root (False).
    persistent_volumes: list[(str, str)]
        List of tuples describing persistent volumes to be mounted in the pod.
        The tuples consist of (PVC Name, Mount Directory).
    """

    @typeguard.typechecked
    def __init__(
        self,
        image: str,
        namespace: str = "default",
        nodes_per_block: int = 1,
        init_blocks: int = 0,
        min_blocks: int = 0,
        max_blocks: int = 10,
        max_cpu: float = 2,
        max_mem: str = "500Mi",
        init_cpu: float = 1,
        init_mem: str = "250Mi",
        parallelism: float = 1,
        worker_init: str = "",
        pod_name: Optional[str] = None,
        user_id: Optional[str] = None,
        group_id: Optional[str] = None,
        run_as_non_root: bool = False,
        secret: Optional[str] = None,
        incluster_config: Optional[bool] = True,
        persistent_volumes: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        if persistent_volumes is None:
            persistent_volumes = []
        if not _kubernetes_enabled:
            raise OptionalModuleMissing(
                ["kubernetes"],
                "Kubernetes provider requires kubernetes module and config.",
            )
        if incluster_config:
            config.load_incluster_config()
        else:
            config.load_kube_config()

        self.namespace = namespace
        self.image = image
        self.nodes_per_block = nodes_per_block
        self.init_blocks = init_blocks

        # Kubernetes provider doesn't really know which pods by container to initialize
        # so best to set init_blocks to 0
        assert init_blocks == 0

        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.max_cpu = max_cpu
        self.max_mem = max_mem
        self.init_cpu = init_cpu
        self.init_mem = init_mem
        self.parallelism = parallelism
        self.worker_init = worker_init
        self.secret = secret
        self.incluster_config = incluster_config
        self.pod_name = pod_name
        self.user_id = user_id
        self.group_id = group_id
        self.run_as_non_root = run_as_non_root
        self.persistent_volumes = persistent_volumes

        self.kube_client = client.CoreV1Api()

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources_by_pod_name: Dict[str, Any] = {}
        # Dictionary that keeps track of jobs, keyed on task_type
        self.resources_by_task_type: Dict[str, Any] = {}

    def submit(self, cmd_string, tasks_per_node, task_type, job_name="funcx-worker"):
        """Submit a job
        Args:
             - cmd_string  :(String) - Name of the container to initiate
             - tasks_per_node (int) : command invocations to be launched per node

        Kwargs:
             - job_name (String): Name for job, must be unique
        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job
        """

        cur_timestamp = str(time.time() * 1000).split(".")[0]
        job_name = f"{job_name}-{cur_timestamp}"

        # Use default image
        image = self.image if task_type == "RAW" else task_type

        # Set the pod name
        if not self.pod_name:
            pod_name = f"{job_name}"
        else:
            pod_name = f"{self.pod_name}-{cur_timestamp}"

        log.debug(f"cmd_string is {cmd_string}")
        formatted_cmd = template_string.format(
            command=cmd_string, worker_init=self.worker_init
        )

        log.info(f"[KUBERNETES] Scaling out a pod with name :{pod_name}")
        self._create_pod(
            image=image,
            pod_name=pod_name,
            job_name=job_name,
            cmd_string=formatted_cmd,
            volumes=self.persistent_volumes,
        )

        self.resources_by_pod_name[pod_name] = {
            "status": "RUNNING",
            "task_type": task_type,
        }
        if task_type not in self.resources_by_task_type:
            self.resources_by_task_type[task_type] = queue.Queue()
        self.resources_by_task_type[task_type].put(pod_name)

        return pod_name

    def status(self, job_ids):
        """Get the status of a list of pods identified by the job identifiers
        returned from the submit request.
        Args:
             - job_ids (list) : A list of job identifiers
        Returns:
             - A dictionary keyed by task_types, containing the count of
               known pods known with that task type.
               For example: {"RAW": 16}
        """
        # This is a hack
        log.debug("Getting Kubernetes provider status")
        status = {}
        for jid in job_ids:
            if jid in self.resources_by_pod_name:
                task_type = self.resources_by_pod_name[jid]["task_type"]
                status[task_type] = status.get(task_type, 0) + 1

        return status

    def cancel(self, num_pods, task_type=None):
        to_kill = []
        if task_type and task_type in self.resources_by_task_type:
            while num_pods > 0:
                try:
                    to_kill.append(
                        self.resources_by_task_type[task_type].get(block=False)
                    )
                except queue.Empty:
                    break
                else:
                    num_pods -= 1
        log.info(f"[KUBERNETES] The to_kill pods are {to_kill}")
        rets = self._cancel(to_kill)
        return to_kill, rets

    def _cancel(self, job_ids):
        """Cancels the jobs specified by a list of job ids
        Args:
        job_ids : [<job_id> ...]
        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        """
        for job in job_ids:
            log.debug(f"Terminating job/proc_id: {job}")
            # Here we are assuming that for local, the job_ids are the process id's
            self._delete_pod(job)

            self.resources_by_pod_name[job]["status"] = "CANCELLED"
            del self.resources_by_pod_name[job]
        log.debug(
            "[KUBERNETES] The resources in kube provider is {}".format(
                self.resources_by_pod_name
            )
        )
        rets = [True for i in job_ids]

        return rets

    def _status(self):
        """Internal: Do not call. Returns the status list for a list of job_ids
        Args:
              self
        Returns:
              [status...] : Status list of all jobs
        """

        # task_type = list(self.resources_by_pod_name.keys())
        # TODO: fix this
        # return jobs_ids
        raise NotImplementedError()
        # do something to get the deployment's status

    def _create_pod(
        self, image, pod_name, job_name, port=80, cmd_string=None, volumes=None
    ):
        """Create a kubernetes pod for the job.
        Args:
              - image (string) : Docker image to launch
              - pod_name (string) : Name of the pod
              - job_name (string) : App label
        KWargs:
             - port (integer) : Container port
        Returns:
              - None
        """
        if volumes is None:
            volumes = []
        security_context = None
        if self.user_id and self.group_id:
            security_context = client.V1SecurityContext(
                run_as_group=self.group_id,
                run_as_user=self.user_id,
                run_as_non_root=self.run_as_non_root,
            )

        # Create the enviornment variables and command to initiate IPP
        environment_vars = client.V1EnvVar(name="TEST", value="SOME DATA")

        launch_args = ["-c", f"{cmd_string}"]

        volume_mounts = []
        # Create mount paths for the volumes
        for volume in volumes:
            volume_mounts.append(
                client.V1VolumeMount(mount_path=volume[1], name=volume[0])
            )
        resources = client.V1ResourceRequirements(
            limits={"cpu": str(self.max_cpu), "memory": self.max_mem},
            requests={"cpu": str(self.init_cpu), "memory": self.init_mem},
        )
        # Configure Pod template container
        container = client.V1Container(
            name=pod_name,
            image=image,
            resources=resources,
            ports=[client.V1ContainerPort(container_port=port)],
            volume_mounts=volume_mounts,
            command=["/bin/bash"],
            args=launch_args,
            env=[environment_vars],
            security_context=security_context,
        )

        # Create a secret to enable pulling images from secure repositories
        secret = None
        if self.secret:
            secret = client.V1LocalObjectReference(name=self.secret)

        # Create list of volumes from (pvc, mount) tuples
        volume_defs = []
        for volume in volumes:
            volume_defs.append(
                client.V1Volume(
                    name=volume[0],
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=volume[0]
                    ),
                )
            )

        metadata = client.V1ObjectMeta(name=pod_name, labels={"app": job_name})
        spec = client.V1PodSpec(
            containers=[container], image_pull_secrets=[secret], volumes=volume_defs
        )

        pod = client.V1Pod(spec=spec, metadata=metadata)
        api_response = self.kube_client.create_namespaced_pod(
            namespace=self.namespace, body=pod
        )
        log.debug(f"Pod created. status='{str(api_response.status)}'")

    def _delete_pod(self, pod_name):
        """Delete a pod"""

        api_response = self.kube_client.delete_namespaced_pod(
            name=pod_name, namespace=self.namespace, body=client.V1DeleteOptions()
        )
        log.debug(f"Pod deleted. status='{str(api_response.status)}'")

    @property
    def label(self):
        return "kubernetes"

    @property
    def status_polling_interval(self):
        return 60
