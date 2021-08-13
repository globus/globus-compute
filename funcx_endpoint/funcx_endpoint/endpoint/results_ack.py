import logging
import time

logger = logging.getLogger(__name__)


class ResultsAckHandler():

    def __init__(self):
        self.unacked_results = {}
        self.unacked_count_log_period = 60
        self.last_unacked_count_log = time.time()
        self.acked_count = 0

    def put(self, task_id, message):
        self.unacked_results[task_id] = message

    def ack(self, task_id):
        acked_task = self.unacked_results.pop(task_id, None)
        if acked_task:
            self.acked_count += 1
            unacked_count = len(self.unacked_results)
            logger.info(f"Acked task {task_id}, Unacked count: {unacked_count}")

    def check_ack_counts(self):
        now = time.time()
        if now - self.last_unacked_count_log > self.unacked_count_log_period:
            unacked_count = len(self.unacked_results)
            logger.info(f"Unacked count: {unacked_count}, Acked results since last check {self.acked_count}")
            self.acked_count = 0
            self.last_unacked_count_log = now

    def handle_resend(self):
        return list(self.unacked_results.values())

    def persist(self):
        # TODO: pickle dump unacked_results
        return
