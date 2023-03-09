from __future__ import annotations

import logging
import multiprocessing
import queue

# multiprocessing.Event is a method, not a class
# to annotate, we need the "real" class
# see: https://github.com/python/typeshed/issues/4266
from multiprocessing.synchronize import Event as EventType

import pika
from globus_compute_endpoint.endpoint.rabbit_mq.base import SubscriberProcessStatus

logger = logging.getLogger(__name__)


class ResultQueueSubscriber(multiprocessing.Process):
    """The ResultQueueSubscriber is a direct rabbitMQ pipe subscriber that uses
    the SelectConnection adaptor to enable performance consumption of messages
    from the service

    """

    QUEUE_NAME = "results"
    ROUTING_KEY = "*.results"
    EXCHANGE_NAME = "results"
    EXCHANGE_TYPE = "topic"

    def __init__(
        self,
        *,
        conn_params: pika.connection.Parameters,
        external_queue: multiprocessing.Queue,
        kill_event: EventType,
    ):
        """

        Parameters
        ----------
        conn_params: Connection Params

        external_queue: multiprocessing.Queue
             Each incoming message will be pushed to the queue.
             Please note that upon pushing a message into this queue, it will be
             marked as delivered.

        kill_event: multiprocessing.Event
             An event object used to signal shutdown to the subscriber process.
        """
        super().__init__()
        self.status = SubscriberProcessStatus.parent

        self.conn_params = conn_params
        self.external_queue = external_queue
        self.kill_event = kill_event
        self.test_class_ready = multiprocessing.Event()
        self._channel_closed = multiprocessing.Event()
        self._cleanup_complete = multiprocessing.Event()
        self._watcher_poll_period_s = 0.1  # seconds

        self._connection = None
        self._channel = None

        logger.debug("Init done")

    def _connect(self) -> pika.SelectConnection:
        logger.info("Connecting to %s", self.conn_params)
        return pika.SelectConnection(
            self.conn_params,
            on_open_callback=self._on_connection_open,
            on_close_callback=self._on_connection_closed,
            on_open_error_callback=self._on_open_failed,
        )

    def _on_connection_open(self, unused_connection):
        logger.debug("Connection opened. Creating a new channel")
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_open_failed(self, *args):
        logger.warning(f"Connection failed to open : {args}")

    def _on_connection_closed(self, connection, exception):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        logger.info("Handling connection closed")
        if self.status is SubscriberProcessStatus.closing or isinstance(
            exception, pika.exceptions.ConnectionClosedByClient
        ):
            logger.info("Closing connection from client")
        elif isinstance(exception, pika.exceptions.ConnectionClosedByBroker):
            logger.warning(f"Connection closed, reopening in 5 seconds: {exception}")
            self._connection.ioloop.call_later(5, self.reconnect)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if self.status is not SubscriberProcessStatus.closing:
            # Create a new connection
            self._connection = self._connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the EXCHANGE_NAME to use.

        :param pika.channel.Channel channel: The channel object

        """
        logger.debug(f"Channel opened: {channel}")
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        logger.info(f"Declaring EXCHANGE_NAME {self.EXCHANGE_NAME}")
        self._channel.exchange_declare(
            exchange=self.EXCHANGE_NAME,
            exchange_type=self.EXCHANGE_TYPE,
            callback=self._on_exchange_declareok,
            durable=True,
        )

    def _on_channel_closed(self, channel, exception):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an EXCHANGE_NAME or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        logger.warning(f"Channel: {channel} was closed: {exception}")
        if isinstance(exception, pika.exceptions.ChannelClosedByBroker):
            logger.warning("Channel closed by RabbitMQ broker")
            self.status = SubscriberProcessStatus.closing
            self.kill_event.set()
        elif isinstance(exception, pika.exceptions.ChannelClosedByClient):
            logger.debug("Detected channel closed by client")
        else:
            logger.exception(
                f"Channel closed with code:{exception.reply_code}, "
                f"error:{exception.reply_text}"
            )
        logger.debug("marking channel as closed")
        self._channel_closed.set()

    def _on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        logger.info("Exchange declared. Declaring queue %s", self.QUEUE_NAME)
        self._channel.queue_declare(
            self.QUEUE_NAME, callback=self._on_queue_declareok, durable=True
        )

    def _on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and EXCHANGE_NAME together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        logger.info(f"Binding EXCHANGE_NAME: {self.EXCHANGE_NAME} to {self.QUEUE_NAME}")

        self._channel.queue_bind(
            queue=self.QUEUE_NAME,
            exchange=self.EXCHANGE_NAME,
            routing_key=self.ROUTING_KEY,
            callback=self._on_bindok,
        )

    def _on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        logger.info("Queue bound")
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
        logger.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        logger.debug("Waiting for basic_consume")
        self.test_class_ready.set()
        self._channel.basic_consume(
            self.QUEUE_NAME, on_message_callback=self.on_message, exclusive=True
        )

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, _properties, body):
        """on_message pushes a message upon receipt into the external_queue
        for async consumption. The message pushed to the external_queue
        is of the following type : Tuple[str, bytes]

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        Pops items into the external_queue of the form:
        tuple(str: Endpoint_UUID, bytes: message)
        """

        routing_key = basic_deliver.routing_key

        if self.external_queue:
            try:
                response: tuple[str, bytes] = (routing_key.rsplit(".", 1)[0], body)
                self.external_queue.put(response)
            except queue.Full:
                logger.exception("Failed to forward message to external_queue")
                self._channel.basic_nack(basic_deliver.delivery_tag, requeue=True)
            else:
                self._channel.basic_ack(delivery_tag=basic_deliver.delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        logger.info("RabbitMQ acknowledged the cancellation of the consumer")
        self._channel.close()

    def _shutdown(self):
        logger.debug("set status to 'closing'")
        self.status = SubscriberProcessStatus.closing
        logger.debug("closing connection")
        self._connection.close()
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
        """Polls the kill_event periodically to trigger a shutdown"""
        if self.kill_event.is_set():
            logger.info("Kill event is set. Start subscriber shutdown")
            try:
                self._shutdown()
            except Exception:
                logger.exception("error while shutting down")
                raise
            logger.info("Shutdown complete")
        else:
            self._connection.ioloop.call_later(
                self._watcher_poll_period_s, self.event_watcher
            )

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self.status = SubscriberProcessStatus.running
        try:
            self._connection = self._connect()
            self.event_watcher()
            self._connection.ioloop.start()
        except Exception:
            logger.exception("Failed to start subscriber")

    def stop(self) -> None:
        """stop() is called by the parent to shutdown the subscriber"""
        logger.info("Stopping")
        self.kill_event.set()
        logger.info("Waiting for cleanup_complete")
        if not self._cleanup_complete.wait(2 * self._watcher_poll_period_s):
            logger.warning("Reached timeout while waiting for cleanup complete")
        # join shouldn't block if the above did not raise a timeout
        self.join()
        self.close()
        logger.info("Cleanup done")
