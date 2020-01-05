import math
import random
import queue
import logging

logger = logging.getLogger("interchange.task_dispatch")
logger.info("Task dispatch started")

def naive_interchange_task_dispatch(interesting_managers,
                                    pending_task_queue,
                                    ready_manager_queue,
                                    scheduler_mode='hard'):
    """
    This is an initial task dispatching algorithm for interchange.
    It returns a dictionary, whose key is manager, and the value is the list of tasks to be sent to manager.
    
    """
    
    if scheduler_mode == 'hard':
        logger.debug("Using hard mode for task dispatching")
        return naive_scheduler_hard(interesting_managers,
                                    pending_task_queue,
                                    ready_manager_queue,
                                    )
    elif scheduler_mode == 'soft':
        logger.debug("Using soft mode for task dispatching")
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
    # The first round: send tasks to the fixed containers on managers
    if interesting_managers and pending_task_queue['total_pending_task_count'] > 0:
        shuffled_managers = list(interesting_managers)
        random.shuffle(shuffled_managers)
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['total_workers'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_hard(pending_task_queue, ready_manager_queue[manager], mode="fixed")
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
                tasks, tids = get_tasks_hard(pending_task_queue, ready_manager_queue[manager], mode="unused")
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


def get_tasks_hard(pending_task_queue, manager_ads, mode='fixed'):
    tasks = []
    tids = []
    if mode != "fixed":
        for task_type in pending_task_queue:
            if task_type == 'total_pending_task_count':
                continue
            for i in range(manager_ads['free_capacity']["unused"]):
                try:
                    x = pending_task_queue[task_type].get(block=False)
                except queue.Empty:
                    break
                else:
                    logger.debug("Get task {}".format(x))
                    tasks.append(x)
                    tids.append(x['task_id'])
                    pending_task_queue['total_pending_task_count'] -= 1
                    manager_ads['free_capacity']['unused'] -= 1
                    manager_ads['free_capacity']['total_workers'] -= 1
        return tasks, tids

    for task_type in manager_ads['free_capacity']:
        if task_type != 'unused' and task_type != 'total_workers':
            for _ in range(manager_ads['free_capacity'][task_type]):
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


def naive_scheduler_soft(interesting_managers,
                         pending_task_queue,
                         ready_manager_queue,
                         ):
    """
    This is an initial task dispatching algorithm for interchange on the soft mode.
    Return a dict to show the tasks to be dispatched to each manager
    """

    task_dispatch = {}
    if interesting_managers and pending_task_queue['total_pending_task_count'] > 0:
        shuffled_managers = list(interesting_managers)
        random.shuffle(shuffled_managers)
        # The first round: send tasks to the fixed containers on managers
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['total_workers'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_hard(ready_manager_queue[manager], mode="fixed")
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

        # The second round: send tasks to the unused containers on managers 
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['total_workers'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_hard(ready_manager_queue[manager], mode="unused")
                ready_manager_queue[manager]['tasks'].extend(tids)
                task_dispatch[manager] = tasks
                logger.info("[MAIN] Assigned tasks {} to manager {}".format(tids, manager))
                if ready_manager_queue[manager]['free_capacity']['total_ready_wrokers'] > 0:
                    logger.debug("[MAIN] Manager {} still has free_capacity {}".format(manager, ready_manager_queue[manager]['free_capacity']['total_workers']))
                else:
                    logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)
    else:
        pass


def get_tasks_soft(pending_task_queue, manager_ads, mode='fixed'):
    tasks = []
    tids = []
    if mode != "fixed":
        for task_type in pending_task_queue:
            for i in range(manager_ads['free_capacity']["unused"]):
                try:
                    x = pending_task_queue[task_type].get(block=False)
                except queue.Empty:
                    break
                else:
                    tasks.append(x)
                    tids.append[x['task_id']]
                    pending_task_queue['total_pending_task_count'] -= 1
                    manager_ads['free_capacity']['unused'] -= 1
                    manager_ads['free_capacity']['total_workers'] -= 1
        return tasks, tids

    for task_type in manager_ads['free_capacity']:
        if task_type != 'unused' and task_type != 'total_workers':
            for _ in range(manager_ads['free_capacity'][task_type]):
                try:
                    x = pending_task_queue[task_type].get(block=False)
                except queue.Empty:
                    break
                else:
                    tasks.append(x)
                    tids.append[x['task_id']]
                    pending_task_queue['total_pending_task_count'] -= 1
                    manager_ads['free_capacity'][task_type] -= 1
                    manager_ads['free_capacity']['total_workers'] -= 1
    return tasks, tids

    

    
    