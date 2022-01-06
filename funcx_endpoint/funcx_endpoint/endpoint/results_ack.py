import logging
import os
import pickle
import time

log = logging.getLogger(__name__)


class ResultsAckHandler:
    """
    Tracks task results by task ID, discarding results after they have been ack'ed
    """

    def __init__(self, endpoint_dir):
        """Initialize results storage and timing for log updates"""
        self.endpoint_dir = endpoint_dir
        self.data_path = os.path.join(self.endpoint_dir, "unacked_results.p")

        self.unacked_results = {}
        # how frequently to log info about acked and unacked results
        self.log_period = 60
        self.last_log_timestamp = time.time()
        self.acked_count = 0

    def put(self, task_id, message):
        """Put sent task result into Unacked Dict

        Parameters
        ----------
        task_id : str
            Task ID

        message : pickled Dict
            Results message
        """
        self.unacked_results[task_id] = message

    def ack(self, task_id):
        """Ack a task result that was sent. Nothing happens if the task ID is not
        present in the Unacked Dict

        Parameters
        ----------
        task_id : str
            Task ID to ack
        """
        acked_task = self.unacked_results.pop(task_id, None)
        if acked_task:
            self.acked_count += 1
            unacked_count = len(self.unacked_results)
            log.debug(f"Acked task {task_id}, Unacked count: {unacked_count}")

    def check_ack_counts(self):
        """Log the number of currently Unacked tasks and the tasks Acked since
        the last check
        """
        now = time.time()
        if now - self.last_log_timestamp > self.log_period:
            unacked_count = len(self.unacked_results)
            log.info(
                "Unacked count: %s, Acked results since last check %s",
                unacked_count,
                self.acked_count,
            )
            self.acked_count = 0
            self.last_log_timestamp = now

    def get_unacked_results_list(self):
        """Get a list of unacked results messages that can be used for resending

        Returns
        -------
        List of pickled Dicts
            Unacked results messages
        """
        return list(self.unacked_results.values())

    def persist(self):
        """Save unacked results to disk"""
        with open(self.data_path, "wb") as fp:
            pickle.dump(self.unacked_results, fp)

    def load(self):
        """Load unacked results from disk"""
        try:
            if os.path.exists(self.data_path):
                with open(self.data_path, "rb") as fp:
                    self.unacked_results = pickle.load(fp)
        except pickle.UnpicklingError:
            log.warning(
                "Cached results %s appear to be corrupt. "
                "Proceeding without loading cached results",
                self.data_path,
            )
