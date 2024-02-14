from __future__ import annotations

import logging
import math
import time

from globus_compute_endpoint.strategies.base import BaseStrategy
from parsl.jobs.states import JobState

log = logging.getLogger(__name__)


class SimpleStrategy(BaseStrategy):
    """Implements the simple strategy"""

    def __init__(self, *args, interval: float = 1.0, max_idletime: float = 60.0):
        """Initialize the flowcontrol object.

        We start the timer thread here

        :param interval: seconds after which timer expires
        :param max_idletime: maximum idle seconds allowed for resources after which
            strategy will try to kill them.  Default: 60s
        """
        log.info("SimpleStrategy Initialized")
        super().__init__(*args, interval=interval)
        self.max_idletime = max_idletime
        self.executors = {"idle_since": None}

        # caching vars; for log noise reduction
        self._prev_info = None
        self._prev_task_breakdown = None
        self._prev_status = None

    def strategize(self, *args, **kwargs):
        try:
            self._strategize(*args, **kwargs)
        except Exception as e:
            log.exception(f"Caught error in strategize : {e}")

    def _strategize(self, *args, **kwargs):
        task_breakdown = self.interchange.get_outstanding_breakdown()
        if task_breakdown != self._prev_task_breakdown:
            self._prev_task_breakdown = task_breakdown
            log.debug(f"Task breakdown {task_breakdown}")

        min_blocks = self.interchange.provider.min_blocks
        max_blocks = self.interchange.provider.max_blocks

        # Here we assume that each node has atleast 4 workers

        tasks_per_node = self.interchange.max_workers_per_node
        if self.interchange.max_workers_per_node == float("inf"):
            tasks_per_node = 1

        nodes_per_block = self.interchange.provider.nodes_per_block
        parallelism = self.interchange.provider.parallelism

        active_tasks = sum(self.interchange.get_total_tasks_outstanding().values())
        status = self.interchange.provider_status()

        running = sum(1 for x in status if x.state == JobState.RUNNING)
        pending = sum(1 for x in status if x.state == JobState.PENDING)
        active_blocks = running + pending
        active_slots = active_blocks * tasks_per_node * nodes_per_block

        status = "; ".join(str(js) for js in status)
        if status != self._prev_status:
            self._prev_status = status
            log.debug(f"Provider states: {status}")

        cur_info = (
            active_tasks,
            running,
            pending,
            self.interchange.get_total_live_workers(),
        )
        if cur_info != self._prev_info:
            self._prev_info = cur_info
            log.debug(
                "Endpoint has %s active tasks, %s/%s running/pending blocks, "
                "and %s connected workers",
                *cur_info,
            )

        # reset kill timer if executor has active tasks
        if active_tasks > 0 and self.executors["idle_since"]:
            self.executors["idle_since"] = None

        # Case 1
        # No tasks.
        if active_tasks == 0:
            # Case 1a
            # Fewer blocks that min_blocks
            if active_blocks <= min_blocks:
                # Ignore
                # log.debug("Strategy: Case.1a")
                pass

            # Case 1b
            # More blocks than min_blocks. Scale down
            else:
                # We want to make sure that max_idletime is reached
                # before killing off resources
                _now = time.monotonic()
                if not self.executors["idle_since"]:
                    log.debug(
                        "Endpoint has 0 active tasks; starting kill timer "
                        "(if idle time exceeds %s seconds, resources will be removed)",
                        self.max_idletime,
                    )
                    self.executors["idle_since"] = _now

                idle_since = self.executors["idle_since"]
                if _now - idle_since > self.max_idletime:
                    # We have resources idle for the max duration,
                    # we have to scale_in now.
                    log.debug(
                        "Idle time has reached {}s; removing resources".format(
                            self.max_idletime
                        )
                    )
                    self.interchange.scale_in(active_blocks - min_blocks)

                else:
                    pass

        # Case 2
        # More tasks than the available slots.
        elif (float(active_slots) / active_tasks) < parallelism:
            # Case 2a
            # We have the max blocks possible
            if active_blocks >= max_blocks:
                # Ignore since we already have the max nodes
                # log.debug("Strategy: Case.2a")
                pass

            # Case 2b
            else:
                # log.debug("Strategy: Case.2b")
                excess = math.ceil((active_tasks * parallelism) - active_slots)
                excess_blocks = math.ceil(
                    float(excess) / (tasks_per_node * nodes_per_block)
                )
                excess_blocks = min(excess_blocks, max_blocks - active_blocks)
                log.debug(f"Requesting {excess_blocks} more blocks")
                self.interchange.scale_out(excess_blocks)

        elif active_slots == 0 and active_tasks > 0:
            # Case 4
            # Check if slots are being lost quickly ?
            log.debug("Requesting single slot")
            if active_blocks < max_blocks:
                self.interchange.scale_out(1)
        # Case 3
        # tasks ~ slots
        else:
            # log.debug("Strategy: Case 3")
            pass
