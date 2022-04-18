import logging

import pika

from .base import RabbitPublisherStatus

logger = logging.getLogger(__name__)


class TaskQueuePublisher:
    """The TaskQueue is a direct rabbitMQ pipe that runs from the service
    to the endpoint.
    Multiple subscribers are disabled, with endpoints using exclusive consume
    Publish side will set heartbeats
    """

    def __init__(
        self,
        *,
        endpoint_id: str,
        conn_params: pika.connection.Parameters,
    ):
        """
        Parameters
        ----------
        endpoint_id: str
             Endpoint UUID string used to identify the endpoint
        conn_params: pika.connection.Parameters
             Pika connection parameters to connect to RabbitMQ
        """
        self.endpoint_id = endpoint_id
        self.queue_name = f"{self.endpoint_id}.tasks"
        self.routing_key = f"{self.endpoint_id}.tasks"
        self.conn_params = conn_params

        self.exchange = "tasks"
        self.exchange_type = "direct"

        self._connection = None
        self._channel = None
        # start closed ("connected" after connect)
        self.status = RabbitPublisherStatus.closed

    def connect(self):
        logger.debug("Connecting as server")
        self._connection = pika.BlockingConnection(self.conn_params)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self.exchange, exchange_type=self.exchange_type
        )
        self._channel.queue_declare(queue=self.queue_name)
        self._channel.queue_bind(self.queue_name, self.exchange)
        self.status = RabbitPublisherStatus.connected

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
        self.status = RabbitPublisherStatus.closed
