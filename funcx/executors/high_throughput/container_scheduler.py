
import math
import random


def naive_scheduler(task_qs, max_workers, logger):
    """ Return two items (as one tuple) dict kill_list :: KILL [(worker_type, num_kill), ...]
                                        dict create_list :: CREATE [(worker_type, num_create), ...]

        In this scheduler model, there is minimum 1 instance of each nonempty task queue.

    """

    logger.debug("Entering scheduler...")
    q_sizes = {}
    q_types = []

    # Sum the size of each task queue
    sum_q_size = 0
    for q_type in task_qs:
        q_types.append(q_type)
        q_size = task_qs[q_type].qsize()
        sum_q_size += q_size
        q_sizes[q_type] = q_size

    logger.info("Total number of tasks is {}".format(sum_q_size))

    # Set proportions of workers equal to the proportion of queue size.
    new_worker_map = {}
    
    if sum_q_size > 0:
        for q_type in q_sizes:
            ratio = q_sizes[q_type]/sum_q_size
            new_worker_map[q_type] = int(math.floor(ratio*max_workers))

    # CLEANUP: Assign the difference here to any random worker. Should be small.
    difference = round(max_workers - sum(new_worker_map.values()))
    logger.info("Offset difference: {}".format(difference))

    logger.info("Queue Types: {}".format(q_types))
    if len(q_types) > 0:
        for i in range(difference):
            win_q = random.choice(q_types)
            print("Winning queue: {}".format(q_types))
            new_worker_map[win_q] += 1

    logger.debug(new_worker_map)
    return new_worker_map
