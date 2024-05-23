from __future__ import annotations

import logging
import os
import queue
import random
import threading
import time
import typing as t
from concurrent.futures import Future

import pika
import pika.channel
import pika.exceptions
from pika.spec import Basic, BasicProperties

from .base import RabbitPublisherStatus

if t.TYPE_CHECKING:
    from pika.channel import Channel
    from pika.frame import Method
    from pika.spec import Exchange
    from pika.spec import Queue as pikaQueue

log = logging.getLogger(__name__)


class ResultPublisher(threading.Thread):
    """
    Publish results to the AMQP service, per the routing information in `queue_info`.
    """

    def __init__(
        self,
        *,
        queue_info: dict,
        poll_period_s: float = 0.5,
        connect_attempt_limit: int = 7200,
        channel_close_window_s: int = 10,
        channel_close_window_limit: int = 3,
    ):
        """
        :param queue_info: Pika connection parameters to connect to RabbitMQ;
            typically as returned from the web-service by the web-service
        :param poll_period_s: [default: 0.5] how frequently to check for and
            handle events.  For example, if the thread should stop or if there
            are results to send along.
        :param connect_attempt_limit: Number of connection attempts to fail before
            giving up.  The connection counter will reset to 0 after the connection
            is sustained for 60s, so transient network errors should not build up
            to a future failure.)
        :param channel_close_window_s: Window of time to count channel close events
        :param channel_close_window_limit: Limit of channel close events (within
            ``channel_close_window_s``) before shutting down the thread.
        """
        self.queue_info = queue_info

        # how often to check for work; every `poll_period_s`, the `_event_watcher`
        # method will handle any outstanding work.
        self.poll_period_s = max(0.01, poll_period_s)

        # how many times to attempt connection before giving up and shutting
        # down the thread
        self.connect_attempt_limit = max(1, connect_attempt_limit)
        self._connection_tries = 0  # count of connection events; reset on success

        # invalid until set in _on_queue_verified
        self._connected_at: float | None = None

        # list of times that channel was last closed; see _on_channel_closed
        self._channel_closes: list[float] = []

        # how long a time frame to keep previous channel close times
        self.channel_close_window_s = channel_close_window_s

        # how many times allowed to retry opening a channel in the above time
        # window before giving up and shutting down the thread
        self.channel_close_window_limit = channel_close_window_limit

        self._mq_conn: pika.SelectConnection | None = None
        self._thread_id = 0  # 0 == not yet started
        self._mq_chan: Channel | None = None
        self._awaiting_confirmation: dict[int, Future] = {}
        self._outstanding: queue.Queue[tuple[Future, bytes]] = queue.Queue()
        self._total_published = 0
        self._delivery_tag_index = 0
        self._stop_event = threading.Event()

        # start closed ("connected" after connect)
        self.status = RabbitPublisherStatus.closed

        publish_kw = dict(**self.queue_info["queue_publish_kwargs"])
        if "properties" in publish_kw:
            publish_kw["properties"] = BasicProperties(**publish_kw["properties"])
        self._publish_kwargs = publish_kw

        super().__init__()

    def __repr__(self):
        return "{}<{}; o:{:,}; t:{:,}>".format(
            type(self).__name__,
            "✓" if self.status == RabbitPublisherStatus.connected else "✗",
            self._outstanding.qsize(),
            self._total_published,
        )

    def run(self) -> None:
        log.debug("%r thread begins", self)
        self._thread_id = threading.get_ident()

        idle_for_s = 0.0
        while (
            not self._stop_event.is_set()
            and self._connection_tries < self.connect_attempt_limit
        ):
            if self._mq_conn or self._connection_tries:
                idle_for_s = random.uniform(0.5, 10)
                msg = f"%r reconnecting in {idle_for_s:.1f}s."
                log.debug(msg, self)
                if self._connection_tries == self.connect_attempt_limit - 1:
                    log.warning(f"{msg}  (final attempt)", self)

            if self._stop_event.wait(idle_for_s):
                break

            self._connection_tries += 1
            try:
                log.debug(
                    "%r Opening connection to AMQP service.  Attempt: %s (of %s)",
                    self,
                    self._connection_tries,
                    self.connect_attempt_limit,
                )
                if self._connection_tries == 1:
                    log.info(f"{self!r} Opening connection to AMQP service.")
                elif self._connection_tries == 2 and not log.isEnabledFor(
                    logging.DEBUG
                ):
                    log.info(
                        f"{self!r} Opening connection to AMQP service (second"
                        " attempt).  Will continue for up to"
                        f" {self.connect_attempt_limit} attempts.  To log all attempts,"
                        " use `--debug`."
                    )
                self._connected_at = None
                self._mq_conn = self._connect()
                self._event_watcher()
                self._mq_conn.ioloop.start()  # Reminder: blocks

            except Exception:
                log.exception("%r Unhandled error; event loop stopped", self)
        self._stop_event.set()

        log.debug("%r thread ends.", self)

    def stop(self, block=True, timeout=5) -> None:
        if threading.get_ident() == self._thread_id:
            log.error("Programming error: invalid attempt to self-shutdown thread.")
            return

        self._stop_event.set()
        if block:
            self.join(timeout=timeout)

    def _connect(self) -> pika.SelectConnection:
        pika_params = pika.URLParameters(self.queue_info["connection_url"])
        return pika.SelectConnection(
            pika_params,
            on_open_callback=self._on_connection_open,
            on_close_callback=self._on_connection_closed,
            on_open_error_callback=self._on_open_failed,
        )

    def _on_open_failed(self, mq_conn: pika.BaseConnection, exc: str | Exception):
        count = f"[attempt {self._connection_tries} (of {self.connect_attempt_limit})]"
        if isinstance(exc, pika.exceptions.ProbableAuthenticationError):
            count = "[invalid credentials; unrecoverable]"
            self._connection_tries = self.connect_attempt_limit

        pid = f"(pid: {os.getpid()})"
        exc_text = f"Failed to open connection - ({exc.__class__.__name__}) {exc}"
        msg = f"{count} {pid} {exc_text}"
        log.debug("%r %s", self, msg)
        if self._connection_tries == 1:
            log.warning(f"{self!r} {msg}")

        if not (self._connection_tries < self.connect_attempt_limit):
            log.error(f"{self!r} {msg}")
            if not isinstance(exc, Exception):
                exc = Exception(str(exc))
        mq_conn.ioloop.stop()

    def _on_connection_closed(self, mq_conn: pika.BaseConnection, exc: Exception):
        msg_fmt = "%r Connection closed: %s"
        log.debug(msg_fmt, self, exc)
        if self._connection_tries == 1:
            # if 1, then we've not been stable for more than 60s (see _event_watcher)
            log.info(msg_fmt, self, exc)
            log.warning(f"{self!r} Unable to sustain connection; retrying ...")

        self.status = RabbitPublisherStatus.closed
        mq_conn.ioloop.stop()

    def _on_connection_open(self, _mq_conn: pika.BaseConnection):
        log.debug("%r Connection established; creating channel", self)
        self._open_channel()

    def _open_channel(self):
        if self._mq_conn.is_open:
            self._mq_conn.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, mq_chan: Channel):
        self._mq_chan = mq_chan
        self._delivery_tag_index = 0

        mq_chan.add_on_close_callback(self._on_channel_closed)
        mq_chan.add_on_cancel_callback(self._on_consumer_cancelled)

        log.debug(
            "Channel %s opened (%s); testing.",
            mq_chan.channel_number,
            mq_chan.connection.params,
        )
        mq_chan.exchange_declare(
            passive=True,
            exchange=self.queue_info["exchange"],
            callback=self._on_exchange_verified,
        )

    def _on_consumer_cancelled(self, frame: Method[Basic.CancelOk]):
        log.info("Consumer cancelled remotely, shutting down: %r", frame)
        if self._mq_chan:
            self._mq_chan.close()

    def _on_channel_closed(self, mq_chan: Channel, exc: Exception):
        self.status = RabbitPublisherStatus.closed
        self._connected_at = None
        now = time.monotonic()
        then = now - self.channel_close_window_s
        self._channel_closes = [cc for cc in self._channel_closes if cc > then]
        self._channel_closes.append(now)
        if len(self._channel_closes) < self.channel_close_window_limit:
            if self._stop_event.is_set():
                return
            msg = f"{self} Channel closed  [{mq_chan}\n  ({exc})]"
            log.debug(msg, exc_info=exc)
            log.warning(msg)
            mq_chan.connection.ioloop.call_later(1, self._open_channel)

        else:
            log.error(
                f"{self} Unable to sustain channel after {len(self._channel_closes)}"
                f" attempts in {self.channel_close_window_limit} seconds. ({exc})"
            )
            self._stop_event.set()

    def _on_exchange_verified(self, _frame: Method[Exchange.DeclareOk]):
        assert self._mq_chan is not None
        self._mq_chan.queue_declare(
            passive=True,
            queue=self.queue_info["queue"],
            callback=self._on_queue_verified,
        )

    def _on_queue_verified(self, _frame: Method[pikaQueue.DeclareOk]):
        assert self._mq_chan is not None
        self._mq_chan.confirm_delivery(self._on_delivery)
        self.status = RabbitPublisherStatus.connected
        self._connected_at = time.time()
        exch = self.queue_info["exchange"]
        log.info(f"{self!r} Ready to send results to exchange: {exch}")

    def _on_delivery(self, frame: Method):
        """
        Complete futures associated frame delivery tag.

        In a SelectConnection setup, the AMQP server may send more than one
        confirmation in a single frame.  In that case, `frame.method.multiple`
        will be `True`, and means that all outstanding message with a lower
        delivery tag index value is also Ack or Nacked.
        """
        is_ack = frame.method.INDEX == Basic.Ack.INDEX
        is_nack = frame.method.INDEX == Basic.Nack.INDEX
        is_multiple = frame.method.multiple
        frame_dtag = frame.method.delivery_tag

        f = self._awaiting_confirmation.pop(frame_dtag)
        if is_ack:
            f.set_result(None)

            if is_multiple:
                for dtag in list(self._awaiting_confirmation.keys()):
                    if dtag < frame_dtag:
                        f = self._awaiting_confirmation.pop(dtag)
                        f.set_result(None)
        elif is_nack:
            exc = Exception(f"Failed to publish delivery_tag {frame_dtag}")
            f.set_exception(exc)
            if is_multiple:
                for dtag in list(self._awaiting_confirmation.keys()):
                    if dtag < frame_dtag:
                        f = self._awaiting_confirmation.pop(dtag)
                        exc = Exception(f"Failed to publish delivery_tag {dtag}")
                        f.set_exception(exc)

    def _event_watcher(self):
        if self._stop_event.is_set():
            log.debug("%r Shutting down due to stop event set.", self)
            self._stop_ioloop()
            return

        if self._connection_tries and self._connected_at:
            # we're connected ...
            if time.time() - self._connected_at > 60:
                # ... and connection stable for 60s; good to reset connection tries
                self._connection_tries = 0
                log.debug(
                    "%r Connection deemed stable; resetting connection tally", self
                )

        try:
            if self.status != RabbitPublisherStatus.connected:
                log.debug("%r not yet connected; holding enqueued messages", self)
                raise queue.Empty()

            while True:
                f, msg_bytes = self._outstanding.get(block=False)
                try:
                    self._mq_chan.basic_publish(body=msg_bytes, **self._publish_kwargs)
                    self._delivery_tag_index += 1
                    self._awaiting_confirmation[self._delivery_tag_index] = f
                    self._total_published += 1
                except pika.exceptions.AMQPError as err:
                    exc = RuntimeError(
                        "Failed to publish result (delivery_tag:"
                        f" {self._delivery_tag_index}"
                    )
                    exc.__cause__ = err
                    f.set_exception(exc)
                    log.error(
                        f"Unable to deliver result to exchange.\n"
                        f"  Error text: {err}\n"
                        f"  kwargs: {self._publish_kwargs}\n"
                        f"  message: {msg_bytes!r}"
                    )
                    continue

        except queue.Empty:
            pass

        finally:
            self._mq_conn.ioloop.call_later(self.poll_period_s, self._event_watcher)

    def _stop_ioloop(self):
        """
        Gracefully stop the ioloop.

        In an effort play nice with upstream, attempt to follow the AMQP protocol
        by closing the channel and connections gracefully.  This method will
        rearm itself while the connection is still open, continually working
        toward eventually and gracefully stopping the connection, before finally
        stopping the ioloop.
        """
        if self._mq_conn:
            self._mq_conn.ioloop.call_later(0.1, self._stop_ioloop)
            if self._mq_conn.is_open:
                if self._mq_chan:
                    if self._mq_chan.is_open:
                        self._mq_chan.close()
                    elif self._mq_chan.is_closed:
                        self._mq_chan = None
                else:
                    self._mq_conn.close()
            elif self._mq_conn.is_closed:
                self._mq_conn.ioloop.stop()
                self._mq_conn = None

    def publish(self, message: bytes) -> Future[None]:
        """
        Enqueue a message to be published over the AMQP channel.  Returns a future
        that can be consulted to determine when the message is acknowledged by the
        AMQP broker.

        :param message: The raw message to send to the broker.

        :raises: ValueError is the publishing thread is not running
        """
        if not self.is_alive():
            raise ValueError("cannot publish(); thread not started")

        f: Future[None] = Future()
        self._outstanding.put((f, message))
        return f
