import logging
import math
import random

from globus_compute_endpoint.logging_config import ComputeLogger

log: ComputeLogger = logging.getLogger(__name__)  # type: ignore


def naive_scheduler(
    task_qs, outstanding_task_count, max_workers, old_worker_map, to_die_list
):
    """
    Return two items (as one tuple)
        dict kill_list :: KILL [(worker_type, num_kill), ...]
        dict create_list :: CREATE [(worker_type, num_create), ...]

    In this scheduler model, there is minimum 1 instance of each nonempty task queue.
    """

    log.trace("Entering scheduler...")
    log.trace("old_worker_map: %s", old_worker_map)
    q_sizes = {}
    q_types = []
    new_worker_map = {}

    # Sum the size of each *available* (unblocked) task queue
    sum_q_size = 0
    for q_type in outstanding_task_count:
        q_types.append(q_type)
        q_size = outstanding_task_count[q_type]
        sum_q_size += q_size
        q_sizes[q_type] = q_size

    if sum_q_size > 0:
        log.info(f"Total number of tasks is {sum_q_size}")

        # Set proportions of workers equal to the proportion of queue size.
        for q_type in q_sizes:
            ratio = q_sizes[q_type] / sum_q_size
            new_worker_map[q_type] = min(
                int(math.floor(ratio * max_workers)), q_sizes[q_type]
            )

        # Check the difference
        tmp_sum_q_size = sum(new_worker_map.values())
        difference = 0
        if sum_q_size > tmp_sum_q_size:
            difference = min(max_workers - tmp_sum_q_size, sum_q_size - tmp_sum_q_size)
        log.debug(f"Offset difference: {difference}")
        log.debug(f"Queue Types: {q_types}")

        if len(q_types) > 0:
            while difference > 0:
                win_q = random.choice(q_types)
                if q_sizes[win_q] > new_worker_map[win_q]:
                    new_worker_map[win_q] += 1
                    difference -= 1

    return new_worker_map
