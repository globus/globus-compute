import logging
import threading
import time

log = logging.getLogger(__name__)


class BaseStrategy:
    """Implements threshold-interval based flow control.

    The overall goal is to trap the flow of apps from the
    workflow, measure it and redirect it the appropriate executors for
    processing.

    This is based on the following logic:

    .. code-block:: none

        BEGIN (INTERVAL, THRESHOLD, callback) :
            start = current_time()

            while (current_time()-start < INTERVAL) :
                 count = get_events_since(start)
                 if count >= THRESHOLD :
                     break

            callback()

    This logic ensures that the callbacks are activated with a maximum delay
    of `interval` for systems with infrequent events as well as systems which would
    generate large bursts of events.

    Once a callback is triggered, the callback generally runs a strategy
    method on the sites available as well asqeuque

    TODO: When the debug logs are enabled this module emits duplicate messages.
    This issue needs more debugging. What I've learnt so far is that the duplicate
    messages are present only when the timer thread is started, so this could be
    from a duplicate logger being added by the thread.
    """

    def __init__(self, *args, threshold=20, interval=5):
        """Initialize the flowcontrol object.

        We start the timer thread here
        Parameters
        ----------
        - threshold (int) : Tasks after which the callback is triggered
        - interval (int) : seconds after which timer expires

        """
        self.interchange = None
        self.threshold = threshold
        self.interval = interval

        self.cb_args = args
        self.callback = self.strategize
        self._handle = None
        self._event_count = 0
        self._event_buffer = []
        self._wake_up_time = time.time() + 1
        self._kill_event = threading.Event()
        self._thread = threading.Thread(
            target=self._wake_up_timer, args=(self._kill_event,), name="Base-Strategy"
        )
        self._thread.daemon = True

    def start(self, interchange):
        """Actually start the strategy
        Parameters
        ----------
        interchange: funcx.executors.high_throughput.interchange.Interchange
            Interchange to bind the strategy to
        """
        self.interchange = interchange
        if hasattr(interchange, "provider"):
            log.debug(
                "Strategy bounds-> init:{}, min:{}, max:{}".format(
                    interchange.provider.init_blocks,
                    interchange.provider.min_blocks,
                    interchange.provider.max_blocks,
                )
            )
        self._thread.start()

    def strategize(self, *args, **kwargs):
        """Strategize is called everytime the threshold or the interval is hit"""
        log.debug(f"Strategize called with {args} {kwargs}")

    def _wake_up_timer(self, kill_event):
        """
        Internal. This is the function that the thread will execute.
        waits on an event so that the thread can make a quick exit when close() is
        called

        Args:
            - kill_event (threading.Event) : Event to wait on
        """

        while True:
            prev = self._wake_up_time

            # Waiting for the event returns True only when the event
            # is set, usually by the parent thread
            time_to_die = kill_event.wait(float(max(prev - time.time(), 0)))

            if time_to_die:
                return

            if prev == self._wake_up_time:
                self.make_callback(kind="timer")
            else:
                print("Sleeping a bit more")

    def notify(self, event_id):
        """Let the FlowControl system know that there is an event.

        This method is to be called from the Interchange to notify the flowcontrol
        """
        self._event_buffer.extend([event_id])
        self._event_count += 1
        if self._event_count >= self.threshold:
            log.debug("Eventcount >= threshold")
            self.make_callback(kind="event")

    def make_callback(self, kind=None):
        """Makes the callback and resets the timer.

        KWargs:
               - kind (str): Default=None, used to pass information on what
                 triggered the callback
        """
        self._wake_up_time = time.time() + self.interval
        self.callback(tasks=self._event_buffer, kind=kind)
        self._event_buffer = []

    def close(self):
        """Merge the threads and terminate."""
        self._kill_event.set()
        self._thread.join()


class Timer:
    """This timer is a simplified version of the FlowControl timer.
    This timer does not employ notify events.

    This is based on the following logic :

    .. code-block:: none


        BEGIN (INTERVAL, THRESHOLD, callback) :
            start = current_time()

            while (current_time()-start < INTERVAL) :
                 wait()
                 break

            callback()

    """

    def __init__(self, callback, *args, interval=5):
        """Initialize the flowcontrol object
        We start the timer thread here

        Args:
             - dfk (DataFlowKernel) : DFK object to track parsl progress

        KWargs:
             - threshold (int) : Tasks after which the callback is triggered
             - interval (int) : seconds after which timer expires
        """

        self.interval = interval
        self.cb_args = args
        self.callback = callback
        self._wake_up_time = time.time() + 1

        self._kill_event = threading.Event()
        self._thread = threading.Thread(
            target=self._wake_up_timer, args=(self._kill_event,), name="Timer"
        )
        self._thread.daemon = True
        self._thread.start()

    def _wake_up_timer(self, kill_event):
        """Internal. This is the function that the thread will execute.
        waits on an event so that the thread can make a quick exit when close() is
        called

        Args:
            - kill_event (threading.Event) : Event to wait on
        """

        # Sleep till time to wake up
        while True:
            prev = self._wake_up_time

            # Waiting for the event returns True only when the event
            # is set, usually by the parent thread
            time_to_die = kill_event.wait(float(max(prev - time.time(), 0)))

            if time_to_die:
                return

            if prev == self._wake_up_time:
                self.make_callback(kind="timer")
            else:
                print("Sleeping a bit more")

    def make_callback(self, kind=None):
        """Makes the callback and resets the timer."""
        self._wake_up_time = time.time() + self.interval
        self.callback(*self.cb_args)

    def close(self):
        """Merge the threads and terminate."""
        self._kill_event.set()
        self._thread.join()
