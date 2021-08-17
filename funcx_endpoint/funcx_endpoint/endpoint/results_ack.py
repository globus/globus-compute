import logging
import time

logger = logging.getLogger(__name__)


class ResultsAckHandler():
    """
    Tracks task results by task ID, discarding results after they have been ack'ed
    """

    def __init__(self):
        """ Initialize results storage and timing for log updates
        """
        self.unacked_results = {}
        # how frequently to log info about acked and unacked results
        self.log_period = 60
        self.last_log_timestamp = time.time()
        self.acked_count = 0

    def put(self, task_id, message):
        """ Put sent task result into Unacked Dict

        Parameters
        ----------
        task_id : str
            Task ID

        message : pickled Dict
            Results message
        """
        self.unacked_results[task_id] = message

    def ack(self, task_id):
        """ Ack a task result that was sent. Nothing happens if the task ID is not
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
            logger.info(f"Acked task {task_id}, Unacked count: {unacked_count}")

    def check_ack_counts(self):
        """ Log the number of currently Unacked tasks and the tasks Acked since
        the last check
        """
        now = time.time()
        if now - self.last_log_timestamp > self.log_period:
            unacked_count = len(self.unacked_results)
            logger.info(f"Unacked count: {unacked_count}, Acked results since last check {self.acked_count}")
            self.acked_count = 0
            self.last_log_timestamp = now

    def get_unacked_results_list(self):
        """ Get a list of unacked results messages that can be used for resending

        Returns
        -------
        List of pickled Dicts
            Unacked results messages
        """
        return list(self.unacked_results.values())

    def persist(self):
        """ Save unacked results to disk
        """
        # TODO: pickle dump unacked_results
        return
