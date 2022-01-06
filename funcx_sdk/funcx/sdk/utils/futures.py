"""Tools for dealing with asynchronous execution

Credit: Logan Ward
"""
from concurrent.futures import Future
from threading import Thread
from time import sleep

from globus_sdk import GlobusAPIError


class FuncXFuture(Future):
    """Utility class for simplifying asynchronous execution in funcX"""

    def __init__(self, client, task_id: str, ping_interval: float):
        """
        Parameters
        ----------
        client : FuncXClient
            Already-initialized client, used to check task status
        task_id : str
            The task id to monitor
        ping_interval : float
             How often to ping the server to check status in seconds
        """
        super().__init__()
        self.client = client
        self.task_id = task_id
        self.ping_interval = ping_interval

        # Once you create this, the task has already started
        self.set_running_or_notify_cancel()

        # Forcing the ping interval to be no less than 1s
        if ping_interval < 1:
            assert AttributeError("Ping interval must be at least 1 second")

        # Start a thread that polls status
        self._checker_thread = Thread(target=FuncXFuture._ping_server, args=(self,))
        self._checker_thread.start()

    def _ping_server(self):
        while True:
            try:
                if not self.running():
                    break
            except GlobusAPIError:
                # Keep pinging even if the results fail
                sleep(self.ping_interval)
                continue
            sleep(self.ping_interval)

    def running(self):
        if super().running():
            status = self.client.get_task_status(self.task_id)
            if status["status"] == "SUCCEEDED":
                self.set_result(status["details"]["result"])
                return False
            if status["status"] == "FAILED":
                try:
                    self.set_result(status["details"]["reason"])
                except Exception:
                    self.set_result(status["details"]["result"])
                return False
            return True
        return False

    # def status(self):
    #     """Get the status of the task
    #
    #     Returns
    #     -------
    #     str
    #         The status of the task
    #     """
    #     self.status, self.result = self.client.get_task_status(self.task_id)
    #     return self.status
    #
    # def result(self):
    #     """
    #     Wait for the task to complete
    #
    #     Returns
    #     -------
    #     dict
    #         The result from the task
    #     """
    #     if not self.result:
    #         while self.status == 'PENDING':
    #             self.status, self.result = self.client.get_task_status(self.task_id)
    #
    #     return self.result

    def cancel(self):
        """Stop the execution of the function"""
        self.set_exception(Exception("Cancelled by user"))
