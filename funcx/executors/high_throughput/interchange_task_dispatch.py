import match
import random
import queue


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
                                    ready_manager_queue)
    elif scheduler_mode == 'soft':
        return naive_scheduler_soft(interesting_managers,
                                    pending_task_queue,
                                    ready_manager_queue)


def naive_scheduler_hard(interesting_managers,
                         pending_task_queue,
                         ready_manager_queue):
    """
    This is an initial task dispatching algorithm for interchange on the hard mode.
    Return a dict to show the tasks to be dispatched to each manager
    """

    task_dispatch = {}
    if interesting_managers and pending_task_queue['total_pending_task_count'] > 0:
        shuffled_managers = list(interesting_managers)
        random.shuffle(shuffled_managers)
        # The first round: send tasks to the fixed containers on managers
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['slots'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_hard(ready_manager_queue[manager], mode="fixed")
                ready_manager_queue[manager]['tasks'].extend(tids)
                task_dispatch[manager] = tasks
                logger.info("[MAIN] Go to send tasks {} to manager {}".format(tids, manager))
                if ready_manager_queue[manager]['free_capacity']['slots'] > 0:
                    logger.debug("[MAIN] Manager {} still has free_capacity {}".format(manager, ready_manager_queue[manager]['free_capacity']['slots']))
                else:
                    logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)

        # The second round: send tasks to the RAW containers on managers 
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['slots'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_hard(ready_manager_queue[manager], mode="RAW")
                ready_manager_queue[manager]['tasks'].extend(tids)
                task_dispatch[manager] = tasks
                logger.info("[MAIN] Going to send tasks {} to manager {}".format(tids, manager))
                if ready_manager_queue[manager]['free_capacity']['slots'] > 0:
                    logger.debug("[MAIN] Manager {} still has free_capacity {}".format(manager, ready_manager_queue[manager]['free_capacity']['slots']))
                else:
                    logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)
    else:
        pass

    return task_dispatch


def get_tasks_hard(pending_task_queue, manager_ads, mode='fixed')
    tasks = []
    tids = []
    if mode != "fixed":
        for task_type in pending_task_queue:
            for i in range(manager_ads['free_capacity']["RAW"]):
                try:
                    x = pending_task_queue[task_type].get(block=False)
                except queue.Empty:
                    break
                else:
                    tasks.append(x)
                    tids.append[x['task_id']]
                    pending_task_queue['total_pending_task_count'] -= 1
                    manager_ads['free_capacity']['RAW'] -= 1
                    manager_ads['free_capacity']['slots'] -= 1
        return tasks, tids

    for task_type in manager_ads['free_capacity']:
        if task_type != 'RAW' and task_type != 'slots':
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
                    manager_ads['free_capacity']['slots'] -= 1
    return tasks, tids


def naive_scheduler_soft(interesting_managers,
                         pending_task_queue,
                         ready_manager_queue):
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
            if (ready_manager_queue[manager]['free_capacity']['slots'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_hard(ready_manager_queue[manager], mode="fixed")
                ready_manager_queue[manager]['tasks'].extend(tids)
                task_dispatch[manager] = tasks
                logger.info("[MAIN] Go to send tasks {} to manager {}".format(tids, manager))
                if ready_manager_queue[manager]['free_capacity']['slots'] > 0:
                    logger.debug("[MAIN] Manager {} still has free_capacity {}".format(manager, ready_manager_queue[manager]['free_capacity']['slots']))
                else:
                    logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)

        # The second round: send tasks to the RAW containers on managers 
        for manager in shuffled_managers:
            if (ready_manager_queue[manager]['free_capacity']['slots'] and
                ready_manager_queue[manager]['active']):
                tasks, tids = get_tasks_hard(ready_manager_queue[manager], mode="RAW")
                ready_manager_queue[manager]['tasks'].extend(tids)
                task_dispatch[manager] = tasks
                logger.info("[MAIN] Going to send tasks {} to manager {}".format(tids, manager))
                if ready_manager_queue[manager]['free_capacity']['slots'] > 0:
                    logger.debug("[MAIN] Manager {} still has free_capacity {}".format(manager, ready_manager_queue[manager]['free_capacity']['slots']))
                else:
                    logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)
    else:
        pass


def get_tasks_soft(pending_task_queue, manager_ads, mode='fixed')
    tasks = []
    tids = []
    if mode != "fixed":
        for task_type in pending_task_queue:
            for i in range(manager_ads['free_capacity']["RAW"]):
                try:
                    x = pending_task_queue[task_type].get(block=False)
                except queue.Empty:
                    break
                else:
                    tasks.append(x)
                    tids.append[x['task_id']]
                    pending_task_queue['total_pending_task_count'] -= 1
                    manager_ads['free_capacity']['RAW'] -= 1
                    manager_ads['free_capacity']['slots'] -= 1
        return tasks, tids

    for task_type in manager_ads['free_capacity']:
        if task_type != 'RAW' and task_type != 'slots':
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
                    manager_ads['free_capacity']['slots'] -= 1
    return tasks, tids

    

    
    