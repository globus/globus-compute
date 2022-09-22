from __future__ import annotations

import logging

import pika
import pika.channel
import pika.exceptions
from pika.adapters.blocking_connection import BlockingChannel

from .base import RabbitPublisherStatus

logger = logging.getLogger(__name__)


class ResultQueuePublisher:
    """ResultPublisher publishes results to a topic EXCHANGE_NAME, with
    the {endpoint_id}.results as a routing key.
    """

    def __init__(
        self,
        *,
        queue_info: dict,
    ):
        """
        Parameters
        ----------
        endpoint_uuid: str
            Endpoint UUID string used to identify the endpoint
        queue_info: pika.connection.Parameters
            Pika connection parameters to connect to RabbitMQ
        """
        self.queue_info = queue_info
        self._channel: BlockingChannel | None = None
        self._connection: pika.BlockingConnection | None = None
        # start closed ("connected" after connect)
        self.status = RabbitPublisherStatus.closed

        publish_kw = dict(**self.queue_info["queue_publish_kwargs"])
        if "properties" in publish_kw:
            publish_kw["properties"] = pika.BasicProperties(**publish_kw["properties"])
        self._publish_kwargs = publish_kw

    def __enter__(self):
        if self.status != RabbitPublisherStatus.connected:
            self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        pika_params = pika.URLParameters(self.queue_info["connection_url"])
        pika_params.heartbeat = 0  # result_q is blocking; no heartbeats warranted
        conn = pika.BlockingConnection(pika_params)
        channel = conn.channel()
        channel.exchange_declare(passive=True, exchange=self.queue_info["exchange"])
        channel.queue_declare(passive=True, queue=self.queue_info["queue"])
        channel.confirm_delivery()

        self._connection = conn
        self._channel = channel
        self.status = RabbitPublisherStatus.connected

    def publish(self, message: bytes) -> None:
        """Publish message to RabbitMQ with the routing key to identify the message
        source.
        The channel specifies confirm_delivery and with `mandatory=True` this call
        will *block* until a delivery confirmation is received.

        Raises: Exception from pika if the message could not be delivered
        """
        if self._channel is None:
            raise ValueError("cannot publish() without first calling connect()")

        try:
            self._channel.basic_publish(body=message, **self._publish_kwargs)
        except pika.exceptions.AMQPError as err:
            logger.error(
                f"Unable to deliver message to exchange.\n"
                f"  Error text: {err}\n"
                f"  kwargs: {self._publish_kwargs}\n"
                f"  message: {message!r}"
            )
            raise

    def close(self):
        if self._connection and self._connection.is_open:
            self._connection.close()
        self.status = RabbitPublisherStatus.closed
