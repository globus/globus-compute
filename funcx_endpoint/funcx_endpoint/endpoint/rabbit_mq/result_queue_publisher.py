import logging

import pika

logger = logging.getLogger(__name__)


class ResultQueuePublisher:
    """ResultPublisher publishes results to a topic EXCHANGE_NAME, with
    the {endpoint_id}.results as a routing key.
    """

    EXCHANGE_NAME = "results"
    EXCHANGE_TYPE = "topic"
    QUEUE_NAME = "results"
    GLOBAL_ROUTING_KEY = "*.results"

    def __init__(
        self,
        endpoint_id: str,
        pika_conn_params: pika.connection.Parameters,
    ):
        """
        Parameters
        ----------
        endpoint_uuid: str
            Endpoint UUID string used to identify the endpoint
        pika_conn_params: pika.connection.Parameters
            Pika connection parameters to connect to RabbitMQ
        """
        self.endpoint_id = endpoint_id
        self.params = pika_conn_params
        if self.params.heartbeat is None:
            # result_q is blocking, and shouldn't use heartbeats
            self.params.heartbeat = 0

        self._channel = None
        self._connection = None

        self.routing_key = f"{self.endpoint_id}.results"

    def connect(self) -> pika.BlockingConnection:
        """Connect

        :rtype: pika.BlockingConnection
        """
        logger.info(f"Connecting to {self.params}")
        self._connection = pika.BlockingConnection(self.params)
        self._channel = self._connection.channel()
        self._channel.confirm_delivery()
        self._channel.exchange_declare(
            exchange=self.EXCHANGE_NAME, exchange_type=self.EXCHANGE_TYPE
        )

        self._channel.queue_declare(queue=self.QUEUE_NAME, passive=True)
        self._channel.queue_bind(
            queue=self.QUEUE_NAME,
            exchange=self.EXCHANGE_NAME,
            routing_key=self.GLOBAL_ROUTING_KEY,
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
                self.EXCHANGE_NAME, self.routing_key, message, mandatory=True
            )
        except pika.exceptions.AMQPError:
            logger.error("Message could not be delivered")
            raise

    def close(self):
        self._channel.close()
        self._connection.close()
