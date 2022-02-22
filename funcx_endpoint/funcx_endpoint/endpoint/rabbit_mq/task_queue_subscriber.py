import logging
import multiprocessing
from typing import Callable

import pika

logger = logging.getLogger(__name__)


class TaskQueueSubscriber(multiprocessing.Process):
    """The TaskQueueSubscriber is a direct rabbitMQ pipe subscriber that uses
    the SelectConnection adaptor to enable performance consumption of messages
    from the service

    """

    def __init__(
        self,
        pika_conn_params: pika.connection.Parameters,
        external_queue: multiprocessing.Queue,
        kill_event: multiprocessing.Event,
        endpoint_uuid: str,
        _on_message_test_hook: Callable = None,
        exchange: str = "tasks",
    ):
        """

        Parameters
        ----------
        pika_conn_params: Connection Params

        external_queue: multiprocessing.Queue
             Each incoming message will be pushed to the queue.
             Please note that upon pushing a message into this queue, it will be
             marked as delivered.

        kill_event: multiprocessing.Event
             This event is used to communicate a failure on the subscriber

        _on_message_test_hook: Callable
            This is a test_hook used only for testing. This is invoked on
            every message

        endpoint_uuid: endpoint uuid string

        exchange: str
            Exchange name
        """

        super().__init__()
        self.endpoint_uuid = endpoint_uuid
        self.conn_params = pika_conn_params
        self.external_queue = external_queue
        self.kill_event: multiprocessing.Event = kill_event
        self.cleanup_complete = multiprocessing.Event()
        self.external_callback = _on_message_test_hook

        self.queue_name = f"{self.endpoint_uuid}.tasks"
        self.routing_key = f"{self.endpoint_uuid}.tasks"
        self.params = pika_conn_params
        self.exchange = exchange
        self.exchange_type = "direct"
        self._closing = False
        self._closed = False
        self._connection = None
        self._channel = None

        self._watcher_poll_period = 0.1  # seconds
        logger.debug("Init done")

    def _connect(self) -> pika.SelectConnection:
        logger.info("Connecting to %s", self.params)
        return pika.SelectConnection(
            self.conn_params,
            on_open_callback=self._on_connection_open,
            on_close_callback=self._on_connection_closed,
            on_open_error_callback=self._on_open_failed,
        )

    def _on_connection_open(self, _unused_connection):
        logger.warning("Connection opened")
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
        if self._closing:
            connection.ioloop.stop()
            self._closed = True
        else:
            logger.warning(f"Connection closed, reopening in 5 seconds: {exception}")
            self._connection.ioloop.call_later(5, self.reconnect)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        logger.info("Creating a new channel")
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        logger.info(f"Channel opened: {channel}")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        logger.info("Adding channel close callback")
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel, exception):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception: exception from pika
        """
        logger.warning(f"Channel:{channel} was closed: ({exception}")
        try:
            logger.warning("Raising exception:")
            raise exception
        except pika.exceptions.ChannelClosedByBroker:
            if "exclusive use" in exception.reply_text:
                logger.exception(
                    "Channel closed by RabbitMQ broker due to exclusive "
                    "ownership by an active endpoint"
                )
                logger.warning("Channel will close without connection retry")
                self._closing = True
        except Exception as e:
            logger.exception(
                f"Channel closed with code:{e.reply_code}, error:{e.reply_text}"
            )

        self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare
        """
        logger.info(f"Declaring exchange {exchange_name}")
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.exchange_type,
            callback=self._on_exchange_declareok,
        )

    def _on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        logger.info("Exchange declared")
        self.setup_queue(self.queue_name)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        logger.info("Declaring queue %s", queue_name)
        self._channel.queue_declare(queue_name, callback=self._on_queue_declareok)

    def _on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        logger.info(f"Binding exchange:{self.exchange} to {self.queue_name}")

        # No routing key since we are doing a direct exchange
        self._channel.queue_bind(
            self.queue_name, self.exchange, callback=self._on_bindok
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
        logger.info("Issuing consumer related RPC commands")
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.queue_name, on_message_callback=self.on_message, exclusive=True
        )

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        logger.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

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
            if self.external_callback:
                self.external_callback(body)

            if self.external_queue:
                self.external_queue.put(body)
        except Exception:
            # We only ack the message if the hand-off to queue worked
            logger.exception("External callback failed")
            self._channel.basic_nack(basic_deliver.delivery_tag, requeue=True)
        else:
            self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        logger.info(f"Acknowledging message {delivery_tag}")
        self._channel.basic_ack(delivery_tag)

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
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        logger.info("Closing the channel")
        self._channel.close()

    def event_watcher(self):
        """Polls the kill_event periodically to trigger a shutdown"""
        if self.kill_event.is_set():
            logger.info("Kill event is set. Start subscriber shutdown")
            try:
                self.close_connection()
                self._connection.ioloop.stop()
                self.cleanup_complete.set()
            except Exception:
                logger.exception("Failed to close connection cleanly")
            logger.info("Shutdown complete")
            return
        else:
            self._connection.ioloop.call_later(
                self._watcher_poll_period, self.event_watcher
            )

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to
        operate.

        Note: Only one of these options should be used.
        """
        logger.warning("Run method")
        try:
            self._connection = self._connect()
            self.event_watcher()
            self._connection.ioloop.start()
        except Exception:
            logger.exception("Failed to start subscriber")

    def close(self) -> None:
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        ^  This doc block is mostly all wrong, since the code has been rewritten
        but some cases described here need to be tested more closely like handling
        KeyboardInterrupts

        This close method needs some rethinking since this whole thing is a separate
        process with the ioloop callbacks not setting the vars properly
        """
        logger.info("Stopping")
        self.kill_event.set()
        self._closing = True
        logger.warning("Waiting for cleanup_complete")
        self.cleanup_complete.wait(2 * self._watcher_poll_period)
        logger.warning("Cleanup done")
        logger.info("Stopped")

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        logger.info("Closing connection")
        return self._connection.close()
