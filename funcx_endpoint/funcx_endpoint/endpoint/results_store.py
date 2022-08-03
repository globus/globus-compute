import glob
import logging
import os
import pickle
import queue
import typing as t

log = logging.getLogger(__name__)


class ResultsStore:
    """Persist results to disk while the Interchange is disconnected from funcX services.
    Reload from disk when reconnected.

    ResultsStore will store results under a directory named "unacked_results_dir",
    with each result being stored as a separate file <task_id>.pkl
    """

    def __init__(self, endpoint_dir: str):
        self.endpoint_dir = endpoint_dir
        self.data_path = os.path.join(self.endpoint_dir, "unacked_results_dir")
        os.makedirs(self.data_path, exist_ok=True)

    def put(self, task_id: str, message: str) -> str:
        """Put a task result that failed to dispatch
        Returns
        -------
        str path to where the result is stored
        """
        result_path = os.path.join(self.data_path, f"{task_id}.pkl")
        with open(result_path, "wb") as f:
            pickle.dump(message, f)
            return result_path

    def get(self) -> t.Tuple[str, str]:
        """Gets one result from the unacked_results_dir, "pop" the entry after to confirm that
        the loaded result was sent and no longer required to be stored.
        If a result is not "pop"ed, calling get will likely return the same result

        Raises
        ------
        queue.Empty when there are no results left in unacked_results_dir

        Returns
        -------
        tuple (task_id, message)
        """
        result_files = glob.glob(f"{self.data_path}/*.pkl")

        if not result_files:
            raise queue.Empty
        result_path = result_files[0]
        task_id = str.split(os.path.basename(result_path), ".")[0]
        with open(result_files[0], "rb") as f:
            message = pickle.load(f)
        return task_id, message

    def pop(self, task_id):
        """Remove the result corresponding to task_id from the store

        Raises
        ------
        FileNotFoundError if requested task is not on disk
        """
        result_path = os.path.join(self.data_path, f"{task_id}.pkl")
        os.remove(result_path)

    def get_stored_tasks_list(self) -> t.List[str]:
        result_files = glob.glob(f"{self.data_path}/*.pkl")
        task_ids = [
            str.split(os.path.basename(rfile), ".")[0] for rfile in result_files
        ]
        return task_ids
