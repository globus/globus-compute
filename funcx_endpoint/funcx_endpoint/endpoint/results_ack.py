import logging
import time

logger = logging.getLogger(__name__)


class ResultsAckHandler():

    def __init__(self):
        self.curr_window = {}
        self.prev_window = {}
        self.curr_window_start = None
        self.window_time = 60

    def put(self, task_id, message):
        self.curr_window[task_id] = message

    def ack(self, task_id):
        acked_task = self.curr_window.pop(task_id, None)
        if acked_task is None:
            acked_task = self.prev_window.pop(task_id, None)
        if acked_task:
            unacked_count = len(self.curr_window) + len(self.prev_window)
            logger.info(f"Acked task {task_id}, Unacked count: {unacked_count}")

    def check_windows(self):
        now = time.time()
        if self.curr_window_start and now - self.curr_window_start > self.window_time:
            if len(self.prev_window) > 0:
                return True

            unacked_count = len(self.curr_window) + len(self.prev_window)
            logger.info(f"Shifting Ack windows, Unacked count: {unacked_count}")

            self.prev_window = self.curr_window
            self.curr_window = {}
            self.curr_window_start = now

        return False

    def handle_resend(self):
        self.curr_window.update(self.prev_window)
        self.prev_window = {}
        self.curr_window_start = time.time()
        return list(self.curr_window.values())
