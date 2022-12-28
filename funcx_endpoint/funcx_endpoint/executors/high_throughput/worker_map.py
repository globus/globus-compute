from __future__ import annotations

import logging
import os
import random
import subprocess
import time
from queue import Empty, Queue
from typing import Any

from funcx_endpoint.logging_config import FXLogger

log: FXLogger = logging.getLogger(__name__)  # type: ignore


class WorkerMap:
    """WorkerMap keeps track of workers"""

    def __init__(
        self,
        max_worker_count: int,
        available_accelerators: list[str],
    ):
        """

        Parameters
        ----------
        max_worker_count:
            Maximum number of workers allowed
        available_accelerators:
            List of accelerator devices workers can be pinned to
        """
        self.max_worker_count = max_worker_count
        self.total_worker_type_counts: dict[str, int] = {
            "unused": self.max_worker_count
        }
        self.ready_worker_type_counts: dict[str, int] = {
            "unused": self.max_worker_count
        }
        self.pending_worker_type_counts: dict[str, Any] = {}
        # a dict to keep track of all the worker_queues with the key of work_type
        self.worker_queues: dict[str, Any] = {}
        # a dict to keep track of all the worker_types with the key of worker_id
        self.worker_types: dict[str, str] = {}
        self.worker_id_counter = 0  # used to create worker_ids

        # Only spin up containers if active_workers + pending_workers < max_workers.
        self.active_workers = 0
        self.pending_workers = 0

        # Need to keep track of workers that are ABOUT to die
        self.to_die_count: dict[str, int] = {}

        # Need to keep track of workers' last idle time by worker type
        self.worker_idle_since: dict[str, float] = {}

        # Create a queue of available accelerators, if accelerators are defined
        self.available_accelerators: Queue | None = None
        if len(available_accelerators) != 0:
            self.available_accelerators = Queue()
            for device in available_accelerators:
                self.available_accelerators.put(device)
        self.assigned_accelerators: dict[str, str] = {}  # Map worker ID -> accelerator

    def register_worker(self, worker_id, worker_type):
        """Add a new worker"""
        log.debug(f"In register worker worker_id: {worker_id} type:{worker_type}")
        self.worker_types[worker_id] = worker_type

        if worker_type not in self.worker_queues:
            self.worker_queues[worker_type] = Queue()

        self.total_worker_type_counts[worker_type] = (
            self.total_worker_type_counts.get(worker_type, 0) + 1
        )
        self.ready_worker_type_counts[worker_type] = (
            self.ready_worker_type_counts.get(worker_type, 0) + 1
        )
        self.pending_worker_type_counts[worker_type] = (
            self.pending_worker_type_counts.get(worker_type, 0) - 1
        )
        self.pending_workers -= 1
        self.active_workers += 1
        self.worker_queues[worker_type].put(worker_id)
        self.worker_idle_since[worker_type] = time.time()

        if worker_type not in self.to_die_count:
            self.to_die_count[worker_type] = 0

    def start_remove_worker(self, worker_type):
        """Increase the to_die_count in prep for a worker getting removed"""
        self.to_die_count[worker_type] += 1

    def remove_worker(self, worker_id):
        """Remove the worker from the WorkerMap

        Should already be KILLed by this point.
        """
        worker_type = self.worker_types[worker_id]
        self.active_workers -= 1
        self.total_worker_type_counts[worker_type] -= 1
        self.to_die_count[worker_type] -= 1
        self.total_worker_type_counts["unused"] += 1
        self.ready_worker_type_counts["unused"] += 1

        # Mark the accelerator as available, if provided
        if worker_id in self.assigned_accelerators:
            device = self.assigned_accelerators.pop(worker_id)
            self.available_accelerators.put(device)

    def spin_up_workers(
        self,
        next_worker_q,
        mode="no_container",
        container_cmd_options="",
        address=None,
        debug=None,
        uid=None,
        logdir=None,
        worker_port=None,
    ):
        """Helper function to call 'remove' for appropriate workers in 'new_worker_map'.

        Parameters
        ----------
        new_worker_q : queue.Queue()
            Queue of worker types to be spun up next.
        mode : str
            Mode of the worker, no_container, singularity, etc.
        address : str
            Address at which to connect to the workers.
        debug : bool
            Whether debug logging is activated.
        uid : str
            Worker ID to be assigned to worker.
        logdir: str
            Directory in which to write logs
        worker_port: int
            Port at which to connect to the workers.

        Returns
        ---------
        Total number of spun-up workers.
        """
        spin_ups = {}

        log.trace("Next Worker Qsize: %s", len(next_worker_q))
        log.trace("Active Workers: %s", self.active_workers)
        log.trace("Pending Workers: %s", self.pending_workers)
        log.trace("Max Worker Count: %s", self.max_worker_count)

        if (
            len(next_worker_q) > 0
            and self.active_workers + self.pending_workers < self.max_worker_count
        ):
            log.debug("Spinning up new workers")
            log.debug(
                "Empty slots: %s",
                self.max_worker_count - self.active_workers - self.pending_workers,
            )
            log.debug(f"New workers: {len(next_worker_q)}")
            log.debug(f"Unused slots: {self.total_worker_type_counts['unused']}")
            num_slots = min(
                self.max_worker_count - self.active_workers - self.pending_workers,
                len(next_worker_q),
                self.total_worker_type_counts["unused"],
            )
            for _ in range(num_slots):

                try:
                    proc = self.add_worker(
                        worker_id=str(self.worker_id_counter),
                        worker_type=next_worker_q.pop(0),
                        container_cmd_options=container_cmd_options,
                        mode=mode,
                        address=address,
                        debug=debug,
                        uid=uid,
                        logdir=logdir,
                        worker_port=worker_port,
                    )
                except Exception:
                    log.exception("Error spinning up worker! Skipping...")
                    continue
                else:
                    spin_ups.update(proc)
        return spin_ups

    def spin_down_workers(
        self,
        new_worker_map,
        worker_max_idletime=60,
        need_more=False,
        scheduler_mode="hard",
    ):
        """Helper function to call 'remove' for appropriate workers in 'new_worker_map'.

        Parameters
        ----------
        new_worker_map : dict
           {worker_type: total_number_of_containers,...}.
        need_more: bool
            whether the manager needs to spin down some warm containers
        Returns
        ---------
        List of removed worker types.
        """
        if need_more:
            return self._spin_down(
                new_worker_map,
                worker_max_idletime=worker_max_idletime,
                scheduler_mode=scheduler_mode,
                check_idle=False,
            )
        else:
            return self._spin_down(
                new_worker_map,
                worker_max_idletime=worker_max_idletime,
                scheduler_mode=scheduler_mode,
                check_idle=True,
            )

    def _spin_down(
        self,
        new_worker_map,
        worker_max_idletime=60,
        scheduler_mode="hard",
        check_idle=True,
    ):
        """Helper function to call 'remove' for appropriate workers in 'new_worker_map'.

        Parameters
        ----------
        new_worker_map : dict
           {worker_type: total_number_of_containers,...}.
        check_idle : boolean
           A boolean to indicate whether to check the idle time of containers or not

           If checked, that means the workloads are not so busy, and we can leave the
           container workers alive until the worker_max_idletime is reached.  Otherwise,
           that means the workloads are busy and we need to turn of some containers to
           acommodate the workers, regardless of if it reaches the worker_max_idletime.

        Returns
        ---------
        List of removed worker types.
        """
        spin_downs = []
        container_switch_count = 0
        now = time.time()
        for worker_type in self.total_worker_type_counts:
            if worker_type == "unused":
                continue
            if (
                check_idle
                and now - self.worker_idle_since[worker_type] < worker_max_idletime
            ):
                log.trace(
                    "Current time: %s (idle since: %s).  Worker type %s has not "
                    "exceeded maximum idle time %s; continuing",
                    now,
                    self.worker_idle_since[worker_type],
                    worker_type,
                    worker_max_idletime,
                )
                continue
            num_remove = max(
                0,
                self.total_worker_type_counts[worker_type]
                - self.to_die_count.get(worker_type, 0)
                - new_worker_map.get(worker_type, 0),
            )
            if scheduler_mode == "hard":
                # Leave at least one worker alive in hard mode
                max_remove = max(0, self.total_worker_type_counts[worker_type] - 1)
                num_remove = min(num_remove, max_remove)

            if num_remove > 0:
                log.debug(f"Removing {num_remove} workers of type {worker_type}")
            for _i in range(num_remove):
                spin_downs.append(worker_type)
            # A container switching is defined as a warm container must be
            # switched to another container to accommodate the workloads.
            # If a container worker has been idle for worker_max_idletime,
            # Then it is not counted as a container switching
            if not check_idle:
                container_switch_count += num_remove
        return spin_downs, container_switch_count

    def add_worker(
        self,
        worker_id=None,
        mode="no_container",
        worker_type="RAW",
        container_cmd_options="",
        walltime=1,
        address=None,
        debug=None,
        worker_port=None,
        logdir=None,
        uid=None,
    ):
        """Launch the appropriate worker

        Parameters
        ----------
        worker_id : str
           Worker identifier string
        mode : str
           Valid options are no_container, singularity
        walltime : int
           Walltime in seconds before we check status

        """
        if worker_id is None:
            str(random.random())

        debug = " --debug" if debug else ""

        worker_id = f" --worker_id {worker_id}"

        self.worker_id_counter += 1

        cmd = (
            f"funcx-worker {debug}{worker_id} "
            f"-a {address} "
            f"-p {worker_port} "
            f"-t {worker_type} "
            f"--logdir={os.path.join(logdir, uid)} "
        )

        container_uri = None
        if worker_type != "RAW":
            container_uri = worker_type

        # If accelerator list is provided, get the next one off the queue
        #   and mark it as assigned
        environment_variables = os.environ.copy()
        if self.available_accelerators is not None:
            try:
                device = self.available_accelerators.get_nowait()
            except Empty:
                raise ValueError(
                    "No accelerators are available."
                    " New worker must be created only"
                    " after another is removed"
                )
            self.assigned_accelerators[worker_id] = device
            log.info(f"Assigned worker '{worker_id}' to accelerator '{device}'")

            # Create the
            #  TODO (wardlt): This code has only been tested for CUDA
            environment_variables["CUDA_VISIBLE_DEVICES"] = device
            environment_variables["ROCR_VISIBLE_DEVICES"] = device
            environment_variables["SYCL_DEVICE_FILTER"] = f"*:*:{device}"

        log.info(f"Command string :\n {cmd}")
        log.info(f"Mode: {mode}")
        log.info(f"Container uri: {container_uri}")
        log.info(f"Container cmd options: {container_cmd_options}")
        log.info(f"Worker type: {worker_type}")

        if mode == "no_container":
            modded_cmd = cmd
        elif mode == "singularity_reuse":
            if container_uri is None:
                log.warning(
                    "No container is specified for singularity mode. "
                    "Spawning a worker in a raw process instead."
                )
                modded_cmd = cmd
            elif not os.path.exists(container_uri):
                log.warning(
                    f"Container uri {container_uri} is not found. "
                    "Spawning a worker in a raw process instead."
                )
                modded_cmd = cmd
            else:
                modded_cmd = (
                    f"singularity exec {container_cmd_options} {container_uri} {cmd}"
                )
            log.info(f"Command string with singularity:\n {modded_cmd}")
        else:
            raise NameError("Invalid container launch mode.")

        try:
            proc = subprocess.Popen(
                modded_cmd.split(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                shell=False,
                env=environment_variables,
            )

        except Exception:
            log.exception("Got an error in worker launch")
            raise

        self.total_worker_type_counts["unused"] -= 1
        self.ready_worker_type_counts["unused"] -= 1
        self.pending_worker_type_counts[worker_type] = (
            self.pending_worker_type_counts.get(worker_type, 0) + 1
        )
        self.pending_workers += 1

        return {str(self.worker_id_counter - 1): proc}

    def get_next_worker_q(self, new_worker_map) -> tuple[list[str], bool]:
        """Helper function to generate a queue of next workers to spin up .
            From a mapping generated by the scheduler

        Parameters
        ----------
        new_worker_map : dict
           {worker_type: total_number_of_containers,...}

        Returns
        ---------
        Queue containing the next workers the system should spin-up.
        """

        # next_worker_q = []
        new_worker_list = []
        log.debug(
            "total_worker_type_counts: %s",
            self.total_worker_type_counts,
        )
        log.debug(
            "pending_worker_type_counts: %s",
            self.pending_worker_type_counts,
        )
        for worker_type in new_worker_map:
            cur_workers = self.total_worker_type_counts.get(
                worker_type, 0
            ) + self.pending_worker_type_counts.get(worker_type, 0)
            if new_worker_map[worker_type] > cur_workers:

                for _i in range(new_worker_map[worker_type] - cur_workers):
                    # Add worker
                    new_worker_list.append(worker_type)

        # need_more is to reflect if a manager needs more workers than the current
        # unused slots
        # If yes, that means the manager needs to turn off some warm workers to serve
        # the requests
        need_more = False
        if len(new_worker_list) > self.total_worker_type_counts["unused"]:
            need_more = True
        # Randomly assign order of newly needed containers... add to spin-up queue.
        if len(new_worker_list) > 0:
            random.shuffle(new_worker_list)

        return new_worker_list, need_more

    def update_worker_idle(self, worker_type):
        """Update the workers' last idle time by worker type"""
        log.debug(f"Worker idle since: {self.worker_idle_since}")
        self.worker_idle_since[worker_type] = time.time()

    def put_worker(self, worker):
        """Adds worker to the list of waiting workers"""
        worker_type = self.worker_types[worker]

        if worker_type not in self.worker_queues:
            self.worker_queues[worker_type] = Queue()

        self.ready_worker_type_counts[worker_type] += 1
        self.worker_queues[worker_type].put(worker)

    def get_worker(self, worker_type):
        """Get a task and reduce the # of worker for that type by 1.
        Raises queue.Empty if empty
        """
        worker = self.worker_queues[worker_type].get_nowait()
        self.ready_worker_type_counts[worker_type] -= 1
        return worker

    def get_worker_counts(self):
        """Returns just the dict of worker_type and counts"""
        return self.total_worker_type_counts

    def ready_worker_count(self):
        return sum(self.ready_worker_type_counts.values())

    def advertisement(self):
        """
        Manager capacity advertisement to interchange.

        The advertisement includes two parts:

        One is the read_worker_type_counts, which reflects the capacity of different
        types of containers on the manager.

        The other is the total number of workers of each type. This includes all the
        pending workers and to_die workers when advertising. We need this "total"
        advertisement because we use killer task mechanisms to kill a worker. When a
        manager is advertising, there may be some killer tasks in queue, and we want to
        ensure that the manager does not over-advertise its actual capacity. Instead,
        let the interchange decide if it is sending too many tasks to the manager.
        """
        ads = {"total": {}, "free": {}}
        total = dict(self.total_worker_type_counts)
        for worker_type in self.pending_worker_type_counts:
            total[worker_type] = (
                total.get(worker_type, 0)
                + self.pending_worker_type_counts[worker_type]
                - self.to_die_count.get(worker_type, 0)
            )
        ads["total"].update(total)
        ads["free"].update(self.ready_worker_type_counts)
        return ads
