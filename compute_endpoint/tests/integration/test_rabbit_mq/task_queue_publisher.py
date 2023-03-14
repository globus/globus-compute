from __future__ import annotations

import logging

import pika
import pika.channel
from globus_compute_endpoint.endpoint.rabbit_mq.base import RabbitPublisherStatus

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
        queue_info: dict,
    ):
        """
        Parameters
        ----------
        endpoint_id: str
             Endpoint UUID string used to identify the endpoint
        conn_params: pika.connection.Parameters
             Pika connection parameters to connect to RabbitMQ
        """
        self.queue_info = queue_info

        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.channel.Channel | None = None
        # start closed ("connected" after connect)
        self.status = RabbitPublisherStatus.closed

    def connect(self):
        logger.debug("Connecting as server")
        params = pika.URLParameters(self.queue_info["connection_url"])
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        self.status = RabbitPublisherStatus.connected

    def publish(self, payload: bytes) -> None:
        """Publish a message to the endpoint from the service

        Parameters
        ----------
        payload: bytes:
            Payload as byte buffer to be published
        """
        if self._channel is None:
            raise ValueError("cannot publish() without first calling connect()")
        self._channel.basic_publish(
            self.queue_info["exchange"],
            routing_key=self.queue_info["queue"],
            body=payload,
            mandatory=True,  # Raise error if message cannot be routed
        )

    def close(self):
        """Close the connection and channels"""
        self._connection.close()
        self.status = RabbitPublisherStatus.closed
