import logging

import pika

logger = logging.getLogger(__name__)


class ResultQueuePublisher:
    """ResultPublisher publishes results to a topic exchange, with the {endpoint_id}.results
    as a routing key.
    """

    def __init__(
        self,
        endpoint_id: str,
        pika_conn_params: pika.connection.Parameters,
        exchange="results",
    ):
        """
        Parameters
        ----------
        endpoint_uuid: str
            Endpoint UUID string used to identify the endpoint
        pika_conn_params: pika.connection.Parameters
            Pika connection parameters to connect to RabbitMQ
        exchange: str
            Exchange name. Default: "result"
        """
        self.endpoint_id = endpoint_id
        self.routing_key = f"{self.endpoint_id}.results"
        self.params = pika_conn_params
        self.params.heartbeat = 30
        self.params.blocked_connection_timeout = 60
        self.exchange = exchange
        self.exchange_type = "topic"
        self._channel = None
        self._connection = None

    def connect(self) -> pika.BlockingConnection:
        """Connect

        :rtype: pika.BlockingConnection
        """
        logger.info(f"Connecting to {self.params}")
        self._connection = pika.BlockingConnection(self.params)
        self._channel = self._connection.channel()
        self._channel.confirm_delivery()
        self._channel.exchange_declare(
            exchange=self.exchange, exchange_type=self.exchange_type
        )
        # TO-DO: This shouldn't be done on the endpoint side
        self._channel.queue_declare(queue="results")
        self._channel.queue_bind(
            queue="results", exchange=self.exchange, routing_key="*.results"
        )

        return self._connection

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
        self._connection.close()

    def _queue_purge(self):
        """This method is *ONLY* for testing. This should not work in production"""
        self._channel.queue_declare(queue="results")
        self._channel.queue_purge("results")
