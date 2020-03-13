
import math
import random


def naive_scheduler(task_qs, max_workers, old_worker_map, to_die_list, logger):
    """ Return two items (as one tuple) dict kill_list :: KILL [(worker_type, num_kill), ...]
                                        dict create_list :: CREATE [(worker_type, num_create), ...]

        In this scheduler model, there is minimum 1 instance of each nonempty task queue.

    """

    logger.debug("Entering scheduler...")
    logger.debug("old_worker_map: {}".format(old_worker_map))
    q_sizes = {}
    q_types = []

    new_worker_map = {}
    # ## Added to disallow rescheduling workers we're waiting to spin down ## #
    blocked_workers = 0
    blocked_types = []
    for w_type in to_die_list:
        if to_die_list[w_type] > 0:
            if old_worker_map is not None:
                blocked_workers += old_worker_map[w_type]  # These workers cannot be replaced.
                blocked_types.append(w_type)
                new_worker_map[w_type] = old_worker_map[w_type]  # Keep the same.
    # ## ****************************************************************# ## #

    # Remove blocked workers from max workers.
    max_workers -= blocked_workers

    # Sum the size of each *available* (unblocked) task queue
    sum_q_size = 0
    for q_type in task_qs:
        if q_type not in blocked_types:
            q_types.append(q_type)
            q_size = task_qs[q_type].qsize()
            sum_q_size += q_size
            q_sizes[q_type] = q_size

    if sum_q_size > 0:
        logger.info("[SCHEDULER] Total number of tasks is {}".format(sum_q_size))

        # Set proportions of workers equal to the proportion of queue size.
        for q_type in q_sizes:
            ratio = q_sizes[q_type] / sum_q_size
            new_worker_map[q_type] = min(int(math.floor(ratio * max_workers)), q_sizes[q_type])

        # CLEANUP: Assign the difference here to any random worker. Should be small.
        logger.info("Temporary new worker map: {}".format(new_worker_map))

        # Check the difference
        tmp_sum_q_size = sum(new_worker_map.values())
        difference = 0
        if sum_q_size > tmp_sum_q_size:
            difference = min(max_workers - tmp_sum_q_size, sum_q_size - tmp_sum_q_size)
        logger.info("[SCHEDULER] Offset difference: {}".format(difference))

        logger.info("[SCHEDULER] Queue Types: {}".format(q_types))
        if len(q_types) > 0:
            while difference > 0:
                win_q = random.choice(q_types)
                if q_sizes(win_q) > new_worker_map[win_q]:
                    new_worker_map[win_q] += 1
                    difference -= 1

        logger.debug("Final new_worker_map: {}".format(new_worker_map))
        return new_worker_map

    else:

        return None


# TODO: Remove after debugging complete.
# from queue import Queue
# task_qs = {'RAW': Queue(), 'even': Queue(), 'odd': Queue()}
# task_qs['RAW'].put(1)
# task_qs['RAW'].put(2)
# task_qs['even'].put(1)
# task_qs['even'].put(2)
# task_qs['odd'].put(1)
# task_qs['odd'].put(2)


# print(naive_scheduler(task_qs, max_workers=12, to_die_list={'RAW':10, 'even':0, 'odd': 0}, old_worker_map={'RAW': 0, 'even': 0, 'odd': 0}, logger=None))
