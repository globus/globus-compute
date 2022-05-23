from __future__ import annotations

import logging

import pika

from .base import RabbitPublisherStatus

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
        *,
        endpoint_id: str,
        conn_params: pika.connection.Parameters,
    ):
        """
        Parameters
        ----------
        endpoint_uuid: str
            Endpoint UUID string used to identify the endpoint
        conn_params: pika.connection.Parameters
            Pika connection parameters to connect to RabbitMQ
        """
        self.endpoint_id = endpoint_id
        self.conn_params = conn_params
        if self.conn_params.heartbeat != 0:
            # result_q is blocking, and shouldn't use heartbeats
            self.conn_params.heartbeat = 0

        self._channel: pika.Channel | None = None
        self._connection: pika.BlockingConnection | None = None
        # start closed ("connected" after connect)
        self.status = RabbitPublisherStatus.closed

        self.routing_key = f"{self.endpoint_id}.results"

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.status == RabbitPublisherStatus.connected:
            self.close()

    def connect(self) -> ResultQueuePublisher:
        self._connection = pika.BlockingConnection(self.conn_params)
        self._channel = self._connection.channel()
        self._channel.confirm_delivery()

        self.status = RabbitPublisherStatus.connected
        return self

    def publish(self, message: bytes) -> None:
        """Publish message to RabbitMQ with the routing key to identify the message source
        The channel specifies confirm_delivery and with `mandatory=True` this call
        will *block* until a delivery confirmation is received.

        Raises: Exception from pika if the message could not be delivered

        """
        if self._channel is None:
            raise ValueError("cannot publish() without first calling connect()")
        try:
            self._channel.basic_publish(
                self.EXCHANGE_NAME, self.routing_key, message, mandatory=True
            )
        except pika.exceptions.AMQPError:
            logger.error("Message could not be delivered")
            raise

    def close(self):
        if self._channel is not None:
            self._channel.close()
        if self._connection is not None:
            self._connection.close()
        self.status = RabbitPublisherStatus.closed
