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
        exchange: str = "tasks",
    ):
        """
        Parameters
        ----------
        endpoint_uuid: str
             Endpoint UUID string used to identify the endpoint
        pika_conn_params: pika.connection.Parameters
             Pika connection parameters to connect to RabbitMQ
        exchange: str
             Exchange name. Default: "tasks"
        """
        self.endpoint_uuid = endpoint_uuid
        self.queue_name = f"{self.endpoint_uuid}.tasks"
        self.routing_key = f"{self.endpoint_uuid}.tasks"
        self.params = pika_conn_params
        self.params._heartbeat = 60
        self.params._blocked_connection_timeout = 120

        self.exchange = exchange
        self.exchange_type = "direct"

        self._conn = None
        self._channel = None

    def connect(self):
        logger.debug("Connecting as server")
        self._conn = pika.BlockingConnection(self.params)
        self._channel = self._conn.channel()
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

    def queue_purge(self):
        """Purge all messages in the queue. Either service/endpoint can
        call this method"""
        self._channel.queue_purge(self.queue_name)

    def close(self):
        """Close channel"""
        self._channel.close()
        self._conn.close()
