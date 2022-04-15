import logging
import math
import time

from funcx_endpoint.strategies.base import BaseStrategy

log = logging.getLogger(__name__)


class KubeSimpleStrategy(BaseStrategy):
    """Implements the simple strategy for Kubernetes"""

    def __init__(self, *args, threshold=20, interval=1, max_idletime=60):
        """Initialize the flowcontrol object.

        We start the timer thread here

        Parameters
        ----------
        threshold:(int)
          Tasks after which the callback is triggered

        interval (int)
          seconds after which timer expires

        max_idletime: (int)
          maximum idle time(seconds) allowed for resources after which strategy will
          try to kill them.
          default: 60s

        """
        log.info("KubeSimpleStrategy Initialized")
        super().__init__(*args, threshold=threshold, interval=interval)
        self.max_idletime = max_idletime
        self.executors_idle_since = {}

    def strategize(self, *args, **kwargs):
        try:
            self._strategize(*args, **kwargs)
        except Exception as e:
            log.exception(f"Caught error in strategize : {e}")

    def _strategize(self, *args, **kwargs):
        max_pods = self.interchange.provider.max_blocks
        min_pods = self.interchange.provider.min_blocks

        # Kubernetes provider only supports one manager in a pod
        managers_per_pod = 1

        workers_per_pod = self.interchange.max_workers_per_node
        if workers_per_pod == float("inf"):
            workers_per_pod = 1

        parallelism = self.interchange.provider.parallelism

        active_tasks = self.interchange.get_total_tasks_outstanding()
        log.trace(f"Pending tasks : {active_tasks}")

        status = self.interchange.provider_status()
        log.trace(f"Provider status : {status}")

        for task_type in active_tasks.keys():
            active_pods = status.get(task_type, 0)
            active_slots = active_pods * workers_per_pod * managers_per_pod
            active_tasks_per_type = active_tasks[task_type]

            log.debug(
                "Endpoint has %s active tasks of %s, %s active blocks, "
                "%s connected workers for %s",
                active_tasks_per_type,
                task_type,
                active_pods,
                self.interchange.get_total_live_workers(),
                task_type,
            )

            # Reset the idle time if we are currently running tasks
            if active_tasks_per_type > 0:
                self.executors_idle_since[task_type] = None

            # Scale down only if there are no active tasks to avoid having to find which
            # workers are unoccupied
            if active_tasks_per_type == 0 and active_pods > min_pods:
                # We want to make sure that max_idletime is reached before killing off
                # resources
                if not self.executors_idle_since[task_type]:
                    log.debug(
                        "Endpoint has 0 active tasks of task type %s; "
                        "starting kill timer (if idle time exceeds %s seconds, "
                        "resources will be removed)",
                        task_type,
                        self.max_idletime,
                    )
                    self.executors_idle_since[task_type] = time.time()

                # If we have resources idle for the max duration we have to scale_in now
                if (
                    time.time() - self.executors_idle_since[task_type]
                ) > self.max_idletime:
                    log.info(
                        "Idle time has reached %s seconds; "
                        "removing resources of task type %s",
                        self.max_idletime,
                        task_type,
                    )
                    self.interchange.scale_in(
                        active_pods - min_pods, task_type=task_type
                    )
            # More tasks than the available slots.
            elif (
                active_tasks_per_type > 0
                and (float(active_slots) / active_tasks_per_type) < parallelism
            ):
                if active_pods < max_pods:
                    excess = math.ceil(
                        (active_tasks_per_type * parallelism) - active_slots
                    )
                    excess_blocks = math.ceil(
                        float(excess) / (workers_per_pod * managers_per_pod)
                    )
                    excess_blocks = min(excess_blocks, max_pods - active_pods)
                    log.info(f"Requesting {excess_blocks} more blocks")
                    self.interchange.scale_out(excess_blocks, task_type=task_type)
            # Immediatly scale if we are stuck with zero pods and work to do
            elif active_slots == 0 and active_tasks_per_type > 0:
                log.info("Requesting single pod")
                self.interchange.scale_out(1, task_type=task_type)
