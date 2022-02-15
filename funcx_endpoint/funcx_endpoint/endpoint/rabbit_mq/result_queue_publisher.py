import logging
import time

import pika

logger = logging.getLogger(__name__)


class ResultQueuePublisher:
    """ResultPublisher publishes results to a topic exchange, with the {endpoint_id}.results
    as a routing key.
    """

    def __init__(
        self,
        endpoint_id: str,
        pika_conn_params: pika.ConnectionParameters,
        exchange="results",
    ):
        self.endpoint_id = endpoint_id
        self.routing_key = f"{self.endpoint_id}.results"
        self.params = pika_conn_params
        self.params.heartbeat = 30
        self.params.blocked_connection_timeout = 60
        self.exchange = exchange
        self.exchange_type = "topic"
        self._is_connected = False
        self._deliveries = None
        self._channel = None
        self._stopping = False

    def connect(self) -> pika.BlockingConnection:
        """Connect

        :rtype: pika.BlockingConnection
        """
        logger.info(f"Connecting to {self.params}")
        self._conn = pika.BlockingConnection(self.params)
        self._channel = self._conn.channel()
        self._channel.confirm_delivery()
        self._channel.exchange_declare(
            exchange=self.exchange, exchange_type=self.exchange_type
        )
        # TO-DO: This shouldn't be done on the endpoint side
        self._channel.queue_declare(queue="results")
        self._channel.queue_bind(
            queue="results", exchange=self.exchange, routing_key="*.results"
        )
        self._is_connected = True

        return self._conn

    def publish_message_retryable(self, message: bytes, retry_count=0, max_retries=3):
        """Publish message to RabbitMQ with the routing key to identify the message source
        The channel specifies confirm_delivery and with `mandatory=True` this call
        will *block* until a delivery confirmation is received.

        """
        try:
            self._channel.basic_publish(
                self.exchange, self.routing_key, message, mandatory=True
            )
        except pika.exceptions.UnroutableError:
            logger.exception("Message could not be delivered.")
            raise
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.ConnectionClosed,
            pika.exceptions.ConnectionBlockedTimeout,
        ):
            logger.exception("Disconnected from the broker, attempting reconnect")
            if retry_count < max_retries:
                time.sleep(3 ** retry_count)  # Hacky exponential back-off
                try:
                    self.connect()
                except Exception:
                    logger.warning(
                        f"Failed to reconnect, attempt {retry_count}/{max_retries}"
                    )
                    return self.publish(message, retry_count=retry_count + 1)
            else:
                raise

    def publish(self, message: bytes) -> None:
        """Publish message to RabbitMQ with the routing key to identify the message source
        The channel specifies confirm_delivery and with `mandatory=True` this call
        will *block* until a delivery confirmation is received.

        Raises: Exception from pika if the message could not be delivered

        """
        try:
            self._channel.basic_publish(
                self.exchange, self.routing_key, message, mandatory=True
            )
        except Exception:
            logger.exception("Message could not be delivered")
            raise

    def close(self):
        self._channel.close()
        self._conn.close()
        self._is_connected = False

    def _queue_purge(self):
        """This method is *ONLY* for testing. This should not work in production"""
        self._channel.queue_declare(queue="results")
        self._channel.queue_purge("results")
