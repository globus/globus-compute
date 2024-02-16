import logging
import threading
import typing as t
from abc import abstractmethod

if t.TYPE_CHECKING:
    from globus_compute_endpoint.engines import GlobusComputeEngine

log = logging.getLogger(__name__)


class BaseStrategy:
    """Implements interval based flow control.

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

    def __init__(self, *args, interval: float = 5.0):
        """Initialize the flowcontrol object.

        We start the timer thread here

        :param interval: how often (in seconds) to invoke self.strategy
        """
        self.interchange: t.Optional[GlobusComputeEngine] = None
        self.interval = interval

        self.cb_args = args
        self._handle = None
        self._kill_event = threading.Event()
        self._thread: t.Optional[threading.Thread] = None

    def start(self, engine: "GlobusComputeEngine"):
        """Actually start the strategy

        :param engine: Engine to which this strategy is bound
        """
        # This thread is created here to ensure a new thread is created whenever start
        # is called. This is to avoid errors from tests reusing strategy objects which
        # would attempt to restart stopped threads.
        self._thread = threading.Thread(
            target=self._wake_up_timer, name="Base-Strategy", daemon=True
        )
        self.interchange = engine
        if hasattr(engine, "provider"):
            log.debug(
                "Strategy bounds-> init:{}, min:{}, max:{}".format(
                    engine.provider.init_blocks,
                    engine.provider.min_blocks,
                    engine.provider.max_blocks,
                )
            )
        self._thread.start()

    @abstractmethod
    def strategize(self, *args, **kwargs):
        """Strategize is called everytime the interval is hit"""
        ...

    def _wake_up_timer(self):
        """
        Internal. This is the function that the thread will execute.
        waits on an event so that the thread can make a quick exit when close() is
        called

        Args:
            - kill_event (threading.Event) : Event to wait on
        """

        while not self._kill_event.wait(self.interval):
            self.make_callback()

    def make_callback(self):
        self.strategize()

    def close(self, timeout: t.Optional[float] = 0.1):
        """Merge the threads and terminate."""
        if self._thread is None:
            return
        self._kill_event.set()
        self._thread.join(timeout=timeout)
