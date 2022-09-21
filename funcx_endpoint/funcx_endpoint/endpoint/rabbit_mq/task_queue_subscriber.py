from __future__ import annotations

import logging
import multiprocessing
import signal

# multiprocessing.Event is a method, not a class
# to annotate, we need the "real" class
# see: https://github.com/python/typeshed/issues/4266
from multiprocessing.synchronize import Event as EventType

import pika
import pika.channel
import pika.exceptions
import pika.frame

from .base import SubscriberProcessStatus

logger = logging.getLogger(__name__)


class TaskQueueSubscriber(multiprocessing.Process):
    """The TaskQueueSubscriber is a direct rabbitMQ pipe subscriber that uses
    the SelectConnection adaptor to enable performance consumption of messages
    from the service

    """

    EXCHANGE_NAME = "tasks"
    EXCHANGE_TYPE = "direct"

    def __init__(
        self,
        *,
        endpoint_id: str,
        queue_info: dict,
        external_queue: multiprocessing.Queue,
        quiesce_event: EventType,
    ):
        """

        Parameters
        ----------
        queue_info: Queue information
             Dictionary that includes the key "connection_url", as well as exchange
             and queue declaration information specified by the server.

        external_queue: multiprocessing.Queue
             Each incoming message will be pushed to the queue.
             Please note that upon pushing a message into this queue, it will be
             marked as delivered.

        quiesce_event: multiprocessing.Event
             This event is used to communicate a failure on the subscriber

        endpoint_id: endpoint uuid string
        """

        super().__init__()
        self.status = SubscriberProcessStatus.parent

        self.endpoint_id = endpoint_id
        self.queue_info = queue_info
        self.external_queue = external_queue
        self.quiesce_event = quiesce_event
        self._channel_closed = multiprocessing.Event()
        self._cleanup_complete = multiprocessing.Event()

        self._connection: pika.SelectConnection | None = None
        self._channel: pika.channel.Channel | None = None
        self._consumer_tag: str | None = None

        self._watcher_poll_period = 0.1  # seconds
        logger.debug("Init done")

    def _connect(self) -> pika.SelectConnection:
        pika_params = pika.URLParameters(self.queue_info["connection_url"])
        return pika.SelectConnection(
            pika_params,
            on_open_callback=self._on_connection_open,
            on_close_callback=self._on_connection_closed,
            on_open_error_callback=self._on_open_failed,
        )

    def _on_connection_open(self, _unused_connection):
        logger.info("Connection opened")
        self.open_channel()

    def _on_open_failed(self, *args):
        logger.warning(f"Connection failed to open : {args}")

    def _on_connection_closed(self, connection, exception):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param exception: Exception object
        """
        logger.warning(f"Connection closing: {exception}")
        self._channel = None
        # Setting the quiesce_event will trigger shutdown via the event_watcher
        self.quiesce_event.set()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if self.status is SubscriberProcessStatus.running:

            # Create a new connection
            self._connection = self._connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        logger.info("Creating a new channel")
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel: pika.channel.Channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the EXCHANGE_NAME to use.

        :param pika.channel.Channel channel: The channel object

        """
        logger.info(f"Channel opened: {channel}")
        self._channel = channel

        logger.debug("Adding channel close callback")
        channel.add_on_close_callback(self._on_channel_closed)

        logger.debug("Adding consumer cancellation callback")
        channel.add_on_cancel_callback(self.on_consumer_cancelled)

        logger.info(f"Ensuring exchange exists: {self.queue_info['exchange']}")
        channel.exchange_declare(
            passive=True,  # *we* don't create the exchange
            callback=self._on_exchange_declareok,
            exchange=self.queue_info["exchange"],
        )

    def _on_channel_closed(self, channel, exception):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an EXCHANGE_NAME or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        This is also invoked at the end of a successful client-initiated close, as when
        `connection.close()` is called and closes any open channels.

        :param pika.channel.Channel: The closed channel
        :param Exception: exception from pika
        """
        logger.warning(f"Channel:{channel} was closed: ({exception}")
        if isinstance(exception, pika.exceptions.ChannelClosedByBroker):
            logger.warning("Channel closed by RabbitMQ broker")
            if "exclusive use" in exception.reply_text:
                logger.exception(
                    "Channel closed by RabbitMQ broker due to exclusive "
                    "ownership by an active endpoint"
                )
                logger.warning("Channel will close without connection retry")
                self.status = SubscriberProcessStatus.closing
                self.quiesce_event.set()
        elif isinstance(exception, pika.exceptions.ChannelClosedByClient):
            logger.debug("Detected channel closed by client")
        else:
            logger.exception("Channel closed by unhandled exception.")
        logger.debug("marking channel as closed")
        self._channel_closed.set()

    def _on_exchange_declareok(self, _frame: pika.frame.Method):
        """
        Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC command.
        """
        logger.info(
            "Exchange declared successfully.  Ensuring queue exists:"
            f" {self.queue_info['queue']}"
        )
        assert self._channel is not None
        self._channel.queue_declare(
            passive=True,  # *we* don't create the queue, just consume it
            callback=self._on_queue_declareok,
            queue=self.queue_info["queue"],
        )

    def _on_queue_declareok(self, _frame: pika.frame.Method):
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        logger.info("Issuing consumer related RPC commands")
        self._consumer_tag = self._channel.basic_consume(
            queue=self.queue_info["queue"],
            on_message_callback=self.on_message,
            exclusive=True,
        )

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def on_message(
        self,
        channel: pika.channel.Channel,
        basic_deliver: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the EXCHANGE_NAME, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.
        """
        logger.debug(
            "Received message from %s: %s, %s",
            basic_deliver.delivery_tag,
            properties.app_id,
            body,
        )

        # Not sure if we need to do this in a locked context,
        # rabbit's ACK system should make sure you don't lose tasks.
        try:
            self.external_queue.put(body)
        except Exception:
            # No sense in waiting for the RMQ default 30m timeout; let it know
            # *now* that this message failed.
            logger.exception("External queue put failed")
            channel.basic_nack(basic_deliver.delivery_tag, requeue=True)
        else:
            channel.basic_ack(basic_deliver.delivery_tag)
            logger.debug("Acknowledged message: %s", basic_deliver.delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(
                consumer_tag=self._consumer_tag, callback=self.on_cancelok
            )

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        logger.info("RabbitMQ acknowledged the cancellation of the consumer")
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        logger.info("Closing the channel")
        self._channel.close()

    def _shutdown(self):
        logger.debug("set status to 'closing'")
        self.status = SubscriberProcessStatus.closing

        if self._connection and self._connection.is_open:
            logger.debug("closing connection")
            try:
                self._connection.close()
            except Exception as exc:
                logger.warning(f"Unexpected error while closing connection: {exc}")

        logger.debug("stopping ioloop")
        self._connection.ioloop.stop()
        logger.debug("waiting until channel is closed (timeout=1 second)")
        if not self._channel_closed.wait(1.0):
            logger.warning("reached timeout while waiting for channel closed")
        logger.debug("closing connection to mp queue")
        self.external_queue.close()
        logger.debug("joining mp queue background thread")
        self.external_queue.join_thread()
        logger.info("shutdown done, setting cleanup event")
        self._cleanup_complete.set()

    def event_watcher(self):
        """Polls the quiesce_event periodically to trigger a shutdown"""
        if self.quiesce_event.is_set():
            logger.info("Shutting down task queue reader due to quiesce event.")
            try:
                self._shutdown()
            except Exception:
                logger.exception("error while shutting down")
                raise
            logger.info("Shutdown complete")
        else:
            self._connection.ioloop.call_later(
                self._watcher_poll_period, self.event_watcher
            )

    def handle_sigterm(self, sig_num, curr_stack_frame):
        logger.warning("Received SIGTERM, setting stop event")
        self.quiesce_event.set()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to
        operate.

        Note: Only one of these options should be used.
        """
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.status = SubscriberProcessStatus.running
        try:
            self._connection = self._connect()
            self.event_watcher()
            self._connection.ioloop.start()
        except Exception:
            logger.exception("Failed to start subscriber")
            self.quiesce_event.set()

    def stop(self) -> None:
        """stop() is called by the parent to shutdown the subscriber"""
        logger.info("Stopping")
        self.quiesce_event.set()
        logger.info("Waiting for cleanup_complete")
        if not self._cleanup_complete.wait(2 * self._watcher_poll_period):
            logger.warning("Reached timeout while waiting for cleanup complete")
        # join shouldn't block if the above did not raise a timeout
        self.join()
        self.close()
        logger.info("Cleanup done")
