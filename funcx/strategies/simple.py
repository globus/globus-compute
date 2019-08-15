from base import BaseStrategy
import logging

logger = logging.getLogger(__name__)

class SimpleStrategy(BaseStrategy):
    """ Implements the simple strategy
    """
    def __init__(self, interchange, *args, threshold=20, interval=5):
        """Initialize the flowcontrol object.

        We start the timer thread here

        Args:
             - dfk (DataFlowKernel) : DFK object to track parsl progress

        KWargs:
             - threshold (int) : Tasks after which the callback is triggered
             - interval (int) : seconds after which timer expires
        """

        super().__init__(interchange, *args, threshold=threshold, interval=interval)
        self.interchange = interchange


    def strategize(self, *args, **kwargs):
        print("[SIMPLE] Start of strategize")
        task_breakdown = self.interchange.get_outstanding_breakdown()
        print(f"[SIMPLE] Task breakdown: {task_breakdown}")
        print("[SIMPLE] Start of strategize")


