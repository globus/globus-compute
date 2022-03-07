import logging

import pika

logger = logging.getLogger(__name__)


class TaskQueuePublisher:
    """The TaskQueue is a direct rabbitMQ pipe that runs from the service
    to the endpoint.
    Multiple subscribers are disabled, with endpoints using exclusive consume
    Publish side will set heartbeats
    """

    def __init__(
        self,
        endpoint_uuid: str,
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
        self.endpoint_uuid = endpoint_uuid
        self.queue_name = f"{self.endpoint_uuid}.tasks"
        self.routing_key = f"{self.endpoint_uuid}.tasks"
        self.params = pika_conn_params

        self.exchange = "tasks"
        self.exchange_type = "direct"

        self._connection = None
        self._channel = None

    def connect(self):
        logger.debug("Connecting as server")
        self._connection = pika.BlockingConnection(self.params)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self.exchange, exchange_type=self.exchange_type
        )
        self._channel.queue_declare(queue=self.queue_name)
        self._channel.queue_bind(self.queue_name, self.exchange)

    def publish(self, payload: bytes):
        """Publish a message to the endpoint from the service

        Parameters
        ----------
        payload: bytes:
            Payload as byte buffer to be published
        """
        return self._channel.basic_publish(
            self.exchange,
            routing_key=self.routing_key,
            body=payload,
            mandatory=True,  # Raise error if message cannot be routed
        )

    def close(self):
        """Close the connection and channels"""
        self._connection.close()
