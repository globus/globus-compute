import math
import random
import queue
import logging
import collections

logger = logging.getLogger("interchange.task_dispatch")
logger.info("Interchange task dispatch started")

def naive_interchange_task_dispatch(interesting_managers,
                                    pending_task_queue,
                                    ready_manager_queue,
                                    scheduler_mode='hard'):
    """
    This is an initial task dispatching algorithm for interchange.
    It returns a dictionary, whose key is manager, and the value is the list of tasks to be sent to manager.
    
    """
    
    if scheduler_mode == 'hard':
        return naive_scheduler_hard(interesting_managers,
                                    pending_task_queue,
                                    ready_manager_queue,
                                    )
    elif scheduler_mode == 'soft':
        return naive_scheduler_soft(interesting_managers,
                                    pending_task_queue,
                                    ready_manager_queue,
                                    )


def naive_scheduler_hard(interesting_managers,
                         pending_task_queue,
                         ready_manager_queue,
                         ):
    """
    This is an initial task dispatching algorithm for interchange on the hard mode.
    Return a dict to show the tasks to be dispatched to each manager
    """

    task_dispatch = {}
    dispatched_task = 0
    # The first round: send tasks to the fixed containers on managers
    if interesting_managers:
        shuffled_managers = list(interesting_managers)
        random.shuffle(shuffled_managers)
        logger.debug("In the first loop of task dispatch")
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['total_workers'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_hard(pending_task_queue, ready_manager_queue[manager], mode="first")
                logger.debug("[MAIN] Get tasks {} from queue".format(tasks))
                if tasks:
                    for task_type in tids:    
                        ready_manager_queue[manager]['tasks'][task_type].update(tids[task_type])
                    task_dispatch[manager] = tasks
                    dispatched_task += len(tasks)
                    logger.debug("[MAIN] Assigned tasks {} to manager {}".format(tids, manager))
                if ready_manager_queue[manager]['free_capacity']['total_workers'] > 0:
                    logger.debug("[MAIN] Manager {} still has free_capacity {}".format(manager, ready_manager_queue[manager]['free_capacity']['total_workers']))
                else:
                    logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)

    # The second round: send tasks to the unused slots on managers
    if interesting_managers:
        shuffled_managers = list(interesting_managers)
        random.shuffle(shuffled_managers)
        logger.debug("In the second loop of task dispatch")
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['total_workers'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_hard(pending_task_queue, ready_manager_queue[manager], mode="second")
                logger.debug("[MAIN] Get tasks {} from queue".format(tasks))
                if tasks:
                    for task_type in tids:    
                        ready_manager_queue[manager]['tasks'][task_type].update(tids[task_type])
                    task_dispatch[manager] = tasks
                    dispatched_task += len(tasks)
                    logger.debug("[MAIN] Assigned tasks {} to manager {}".format(tids, manager))
                if ready_manager_queue[manager]['free_capacity']['total_workers'] > 0:
                    logger.debug("[MAIN] Manager {} still has free_capacity {}".format(manager, ready_manager_queue[manager]['free_capacity']['total_workers']))
                else:
                    logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)
    else:
        pass

    logger.debug("The task dispatch is {}, in total {} tasks".format(task_dispatch, dispatched_task))
    return task_dispatch, dispatched_task


def get_tasks_hard(pending_task_queue, manager_ads, mode='first'):
    tasks = []
    tids = collections.defaultdict(set)
    # allocate unused slots to tasks
    if mode != "first":
        for task_type in pending_task_queue:
            while manager_ads['free_capacity']["unused"] > 0:
                try:
                    x = pending_task_queue[task_type].get(block=False)
                except queue.Empty:
                    break
                else:
                    logger.debug("Get task {}".format(x))
                    tasks.append(x)
                    tids[task_type].add(x['task_id'])
                    manager_ads['free_capacity']['unused'] -= 1
                    manager_ads['free_capacity']['total_workers'] -= 1
        return tasks, tids

    for task_type in manager_ads['free_capacity']:
        if task_type != 'unused' and task_type != 'total_workers':
            while manager_ads['free_capacity'][task_type] > 0:
                try:
                    if task_type not in pending_task_queue:
                        break
                    x = pending_task_queue[task_type].get(block=False)
                except queue.Empty:
                    break
                else:
                    logger.debug("Get task {}".format(x))
                    tasks.append(x)
                    tids[task_type].add(x['task_id'])                    
                    manager_ads['free_capacity'][task_type] -= 1
                    manager_ads['free_capacity']['total_workers'] -= 1
    return tasks, tids


def naive_scheduler_soft(interesting_managers,
                         pending_task_queue,
                         ready_manager_queue,
                         ):
    """
    This is an initial task dispatching algorithm for interchange on the soft mode.
    Return a dict to show the tasks to be dispatched to each manager
    """

    task_dispatch = {}
    # The first round: send tasks to the fixed containers on managers to avoid 
    if interesting_managers and pending_task_queue['total_pending_task_count'] > 0:
        shuffled_managers = list(interesting_managers)
        random.shuffle(shuffled_managers)
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['total_workers'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_soft(pending_task_queue, ready_manager_queue[manager], mode="first")
                logger.info("[MAIN] Get tasks {} from queue".format(tasks))
                if tasks:
                    ready_manager_queue[manager]['tasks'].extend(tids)
                    task_dispatch[manager] = tasks
                    logger.info("[MAIN] Assigned tasks {} to manager {}".format(tids, manager))
                if ready_manager_queue[manager]['free_capacity']['total_workers'] > 0:
                    logger.debug("[MAIN] Manager {} still has free_capacity {}".format(manager, ready_manager_queue[manager]['free_capacity']['total_workers']))
                else:
                    logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)

    # The second round: send tasks to the unused slots on managers 
    if interesting_managers and pending_task_queue['total_pending_task_count'] > 0:
        shuffled_managers = list(interesting_managers)
        random.shuffle(shuffled_managers)
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['total_workers'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_soft(pending_task_queue, ready_manager_queue[manager], mode="second")
                logger.info("[MAIN] Get tasks {} from queue".format(tasks))
                if tasks:
                    ready_manager_queue[manager]['tasks'].extend(tids)
                    task_dispatch[manager] = tasks
                    logger.info("[MAIN] Assigned tasks {} to manager {}".format(tids, manager))
                if ready_manager_queue[manager]['free_capacity']['total_workers'] > 0:
                    logger.debug("[MAIN] Manager {} still has free_capacity {}".format(manager, ready_manager_queue[manager]['free_capacity']['total_workers']))
                else:
                    logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)
    else:
        pass

    logger.debug("The task dispatch is {}".format(task_dispatch))
    return task_dispatch


def get_tasks_soft(pending_task_queue, manager_ads, mode='first'):
    tasks = []
    tids = []
    if mode != "first":
        for task_type in pending_task_queue:
            while manager_ads['free_capacity']['total_workers'] > 0:
                try:
                    x = pending_task_queue[task_type].get(block=False)
                except queue.Empty:
                    break
                else:
                    logger.debug("Get task {}".format(x))
                    tasks.append(x)
                    tids.append(x['task_id'])
                    pending_task_queue['total_pending_task_count'] -= 1
                    manager_ads['free_capacity'][task_type] = manager_ads['free_capacity'].get(task_type, 0) - 1
                    manager_ads['free_capacity']['total_workers'] -= 1
        return tasks, tids

    for task_type in manager_ads['free_capacity']:
        if task_type != 'unused' and task_type != 'total_workers':
            while manager_ads['free_capacity'][task_type] > 0:
                try:
                    x = pending_task_queue[task_type].get(block=False)
                except queue.Empty:
                    break
                else:
                    logger.debug("Get task {}".format(x))
                    tasks.append(x)
                    tids.append(x['task_id'])
                    pending_task_queue['total_pending_task_count'] -= 1
                    manager_ads['free_capacity'][task_type] -= 1
                    manager_ads['free_capacity']['total_workers'] -= 1
    return tasks, tids
    
    
