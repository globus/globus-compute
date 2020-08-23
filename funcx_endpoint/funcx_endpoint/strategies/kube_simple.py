from funcx_endpoint.strategies.base import BaseStrategy
import math
import logging
import time
import collections

logger = logging.getLogger("interchange.strategy.KubeSimple")


class KubeSimpleStrategy(BaseStrategy):
    """ Implements the simple strategy for Kubernetes
    """

    def __init__(self, *args,
                 threshold=20,
                 interval=1,
                 max_idletime=60):
        """Initialize the flowcontrol object.

        We start the timer thread here

        Parameters
        ----------
        threshold:(int)
          Tasks after which the callback is triggered

        interval (int)
          seconds after which timer expires

        max_idletime: (int)
          maximum idle time(seconds) allowed for resources after which strategy will try to kill them.
          default: 60s

        """
        logger.info("KubeSimpleStrategy Initialized")
        super().__init__(*args, threshold=threshold, interval=interval)
        self.max_idletime = max_idletime
        self.executors_idle_since = {}

    def strategize(self, *args, **kwargs):
        try:
            self._strategize(*args, **kwargs)
        except Exception as e:
            logger.exception("Caught error in strategize : {}".format(e))
            pass

    def _strategize(self, *args, **kwargs):
        task_breakdown = self.interchange.get_outstanding_breakdown()
        logger.info(f"Task breakdown {task_breakdown}")

        min_blocks = self.interchange.config.provider.min_blocks
        max_blocks = self.interchange.config.provider.max_blocks

        # Here we assume that each node has atleast 4 workers

        tasks_per_node = self.interchange.config.max_workers_per_node
        if self.interchange.config.max_workers_per_node == float('inf'):
            tasks_per_node = 1

        nodes_per_block = self.interchange.config.provider.nodes_per_block
        parallelism = self.interchange.config.provider.parallelism

        active_tasks = self.interchange.get_total_tasks_outstanding()
        logger.debug(f"Pending tasks : {active_tasks}")
        status = self.interchange.provider_status()
        logger.debug(f"Provider status : {status}")

        for task_type in active_tasks:
            active_blocks = status.get(task_type, 0)
            active_slots = active_blocks * tasks_per_node * nodes_per_block
            active_tasks_per_type = active_tasks[task_type]

            logger.debug('Endpoint has {} active tasks of {}, {} active blocks, {} connected workers'.format(
                active_tasks_per_type, task_type, active_blocks, self.interchange.get_total_live_workers()))

            if active_tasks_per_type > 0:
                self.executors_idle_since[task_type] = None

            # Case 1
            # No tasks.
            if active_tasks_per_type == 0:
                # Case 1a
                # Fewer blocks that min_blocks
                if active_blocks <= min_blocks:
                    # Ignore
                    # logger.debug("Strategy: Case.1a")
                    pass

                # Case 1b
                # More blocks than min_blocks. Scale down
                else:
                    # We want to make sure that max_idletime is reached
                    # before killing off resources
                    if not self.executors_idle_since[task_type]:
                        logger.debug(
                            f"Endpoint has 0 active tasks of task type {task_type}; starting kill timer (if idle time "
                            f"exceeds {self.max_idletime}s, resources will be removed)"
                        )
                        self.executors_idle_since[task_type] = time.time()

                    idle_since = self.executors_idle_since[task_type]
                    if (time.time() - idle_since) > self.max_idletime:
                        # We have resources idle for the max duration,
                        # we have to scale_in now.
                        logger.info("Idle time has reached {}s; removing resources of task type {}".format(
                            self.max_idletime, task_type)
                        )
                        self.interchange.scale_in(active_blocks - min_blocks, task_type=task_type)

                    else:
                        pass
                        # logger.debug("Strategy: Case.1b. Waiting for timer : {0}".format(idle_since))

            # Case 2
            # More tasks than the available slots.
            elif (float(active_slots) / active_tasks_per_type) < parallelism:
                # Case 2a
                # We have the max blocks possible
                if active_blocks >= max_blocks:
                    # Ignore since we already have the max nodes
                    # logger.debug("Strategy: Case.2a")
                    pass

                # Case 2b
                else:
                    # logger.debug("Strategy: Case.2b")
                    excess = math.ceil((active_tasks_per_type * parallelism) - active_slots)
                    excess_blocks = math.ceil(float(excess) / (tasks_per_node * nodes_per_block))
                    excess_blocks = min(excess_blocks, max_blocks - active_blocks)
                    logger.info("Requesting {} more blocks".format(excess_blocks))
                    self.interchange.scale_out(excess_blocks, task_type=task_type)

            elif active_slots == 0 and active_tasks_per_type > 0:
                # Case 4
                # Check if slots are being lost quickly ?
                logger.info("Requesting single slot")
                if active_blocks < max_blocks:
                    self.interchange.scale_out(1, task_type=task_type)
            # Case 3
            # tasks ~ slots
            else:
                # logger.debug("Strategy: Case 3")
                pass
