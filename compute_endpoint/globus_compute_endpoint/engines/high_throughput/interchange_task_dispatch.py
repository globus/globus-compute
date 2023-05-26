from __future__ import annotations

import collections
import logging
import queue
import random

from globus_compute_endpoint.logging_config import ComputeLogger

log: ComputeLogger = logging.getLogger(__name__)  # type: ignore
log.info("Interchange task dispatch started")


def naive_interchange_task_dispatch(
    interesting_managers: set[bytes],
    pending_task_queue: dict[str, queue.Queue[dict]],
    ready_manager_queue: dict[bytes, dict],
    scheduler_mode: str = "hard",
    cold_routing: bool = False,
) -> tuple[dict[bytes, list], int]:
    """
    This is an initial task dispatching algorithm for interchange.
    It returns a dictionary, whose key is manager, and the value is the list of tasks
    to be sent to manager, and the total number of dispatched tasks.
    """
    task_dispatch: dict[bytes, list] = {}
    dispatched_tasks = 0
    if scheduler_mode == "hard":
        dispatched_tasks += dispatch(
            task_dispatch,
            interesting_managers,
            pending_task_queue,
            ready_manager_queue,
            scheduler_mode="hard",
        )

    elif scheduler_mode == "soft":
        loops = ["warm"] if not cold_routing else ["warm", "cold"]
        for loop in loops:
            dispatched_tasks += dispatch(
                task_dispatch,
                interesting_managers,
                pending_task_queue,
                ready_manager_queue,
                scheduler_mode="soft",
                loop=loop,
            )
    return task_dispatch, dispatched_tasks


def dispatch(
    task_dispatch: dict[bytes, list],
    interesting_managers: set[bytes],
    pending_task_queue: dict[str, queue.Queue[dict]],
    ready_manager_queue: dict[bytes, dict],
    scheduler_mode: str = "hard",
    loop: str = "warm",
) -> int:
    """
    This is the core task dispatching algorithm for interchange.
    The algorithm depends on the scheduler mode and which loop.
    """
    dispatched_tasks = 0
    if interesting_managers:
        shuffled_managers = list(interesting_managers)
        random.shuffle(shuffled_managers)
        for manager in shuffled_managers:
            mdata = ready_manager_queue[manager]
            tasks_inflight = mdata["total_tasks"]
            real_capacity: int = min(
                mdata["free_capacity"]["total_workers"],
                mdata["max_worker_count"] - tasks_inflight,
            )
            if not (real_capacity > 0 and mdata["active"]):
                interesting_managers.remove(manager)
                continue

            if scheduler_mode == "hard":
                tasks, tids = get_tasks_hard(pending_task_queue, mdata, real_capacity)
            else:
                tasks, tids = get_tasks_soft(
                    pending_task_queue,
                    mdata,
                    real_capacity,
                    loop=loop,
                )
            if tasks:
                log.debug("Got %s tasks from queue", len(tasks))
                for task_type in tids:
                    # This line is a set update, not dict update
                    mdata["tasks"][task_type].update(tids[task_type])
                log.debug(f"The tasks on manager %s is {mdata['tasks']}", manager)
                mdata["total_tasks"] += len(tasks)
                if manager not in task_dispatch:
                    task_dispatch[manager] = []
                task_dispatch[manager] += tasks
                dispatched_tasks += len(tasks)
                log.debug("Assigned tasks %s to manager %s", tids, manager)
            if mdata["free_capacity"]["total_workers"] > 0:
                log.trace(
                    "Manager %s still has free_capacity %s",
                    manager,
                    mdata["free_capacity"]["total_workers"],
                )
            else:
                log.debug("Manager %s is now saturated", manager)
                interesting_managers.remove(manager)

    log.trace(
        "The task dispatch of %s loop is %s, in total %s tasks",
        loop,
        task_dispatch,
        dispatched_tasks,
    )
    return dispatched_tasks


def get_tasks_hard(
    pending_task_queue: dict[str, queue.Queue[dict]],
    manager_ads: dict,
    real_capacity: int,
) -> tuple[list[dict], dict[str, set[str]]]:
    tasks: list[dict] = []
    tids: dict[str, set[str]] = collections.defaultdict(set)
    task_type: str = manager_ads["worker_type"]
    if not task_type:
        log.warning(
            "Using hard scheduler mode but with manager worker type unset. "
            "Use soft scheduler mode. Set this in the config."
        )
        return tasks, tids
    task_q = pending_task_queue.get(task_type)
    if not task_q:
        log.trace("No task of type %s. Exiting task fetching.", task_type)
        return tasks, tids

    # dispatch tasks of available types on manager
    free_cap = manager_ads["free_capacity"]
    if task_type in free_cap["free"]:
        try:
            while real_capacity > 0 and free_cap["free"][task_type] > 0:
                x = task_q.get(block=False)
                log.debug(f"Get task {x}")
                tasks.append(x)
                tids[task_type].add(x["task_id"])
                free_cap["free"][task_type] -= 1
                free_cap["total_workers"] -= 1
                real_capacity -= 1
        except queue.Empty:
            pass

    # dispatch tasks to unused slots based on the manager type
    log.trace("Second round of task fetching in hard mode")
    try:
        while real_capacity > 0 and free_cap["free"]["unused"] > 0:
            x = task_q.get(block=False)
            log.debug(f"Get task {x}")
            tasks.append(x)
            tids[task_type].add(x["task_id"])
            free_cap["free"]["unused"] -= 1
            free_cap["total_workers"] -= 1
            real_capacity -= 1
    except queue.Empty:
        pass
    return tasks, tids


def get_tasks_soft(
    pending_task_queue: dict[str, queue.Queue[dict]],
    manager_ads: dict,
    real_capacity: int,
    loop: str = "warm",
) -> tuple[list[dict], dict[str, set[str]]]:
    tasks = []
    tids = collections.defaultdict(set)

    # Warm routing to dispatch tasks
    free_cap = manager_ads["free_capacity"]
    if loop == "warm":
        for task_type in free_cap["free"]:
            # Dispatch tasks that are of the available container types on the manager
            if task_type != "unused":
                task_q = pending_task_queue.get(task_type)
                if not task_q:
                    continue
                type_inflight = len(manager_ads["tasks"].get(task_type, set()))
                type_capacity = min(
                    free_cap["free"][task_type],
                    free_cap["total"][task_type] - type_inflight,
                )
                try:
                    while (
                        real_capacity > 0
                        and type_capacity > 0
                        and free_cap["free"][task_type] > 0
                    ):
                        x = task_q.get(block=False)
                        log.debug(f"Get task {x}")
                        tasks.append(x)
                        tids[task_type].add(x["task_id"])
                        free_cap["free"][task_type] -= 1
                        free_cap["total_workers"] -= 1
                        real_capacity -= 1
                        type_capacity -= 1
                except queue.Empty:
                    pass
            # Dispatch tasks to unused container slots on the manager
            else:
                task_q = pending_task_queue.get(task_type)  # "unused" queue
                if not task_q:
                    log.debug("Unexpectedly non-existent 'unused' queue")
                    continue
                task_types = list(pending_task_queue.keys())
                random.shuffle(task_types)
                for task_type in task_types:
                    try:
                        while (
                            real_capacity > 0
                            and free_cap["free"]["unused"] > 0
                            and free_cap["total_workers"] > 0
                        ):
                            x = task_q.get(block=False)
                            log.debug(f"Get task {x}")
                            tasks.append(x)
                            tids[task_type].add(x["task_id"])
                            free_cap["free"]["unused"] -= 1
                            free_cap["total_workers"] -= 1
                            real_capacity -= 1
                    except queue.Empty:
                        pass
        return tasks, tids

    # Cold routing round: allocate tasks of random types
    # to workers that are of different types on the manager
    # This will possibly cause container switching on the manager
    # This is needed to avoid workers being idle for too long
    # Potential issues may be that it could kill containers of short tasks frequently
    # Tune cold_routing_interval in the config to balance such a tradeoff
    log.debug("Cold function routing!")
    task_types = list(pending_task_queue.keys())
    random.shuffle(task_types)
    for task_type in task_types:
        task_q = pending_task_queue.get(task_type)
        if not task_q:
            continue
        try:
            while real_capacity > 0 and free_cap["total_workers"] > 0:
                x = task_q.get(block=False)
                tasks.append(x)
                tids[task_type].add(x["task_id"])
                free_cap["total_workers"] -= 1
                real_capacity -= 1
                log.debug(f"Get task {x}")
        except queue.Empty:
            pass
    return tasks, tids
