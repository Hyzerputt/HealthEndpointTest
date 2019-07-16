#! Python 3.7
"""
Library for interacting with RabbitMQ as the DKIST interservice bus.
Breakdown into separate consumer and producer objects enforces the
best practice of separating consumer and producer connections for dynamic
throttling by RabbitMQ.
"""
from typing import Callable, List
import json

import pika
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosed,
    ChannelWrongStateError,
    ConnectionClosed,
    ConnectionWrongStateError,
    StreamLostError
)
from loguru import logger

from .utils.retry import retry_call


__all__ = ["ConsumerConnector", "ProducerConnector"]


SECONDS_60 = 60
RETRY_INDEFINITELY = -1
RETRY_OFF = 1
RABBITMQ_RETRY_CONFIG = {
    "exceptions": (AMQPConnectionError,
                   ChannelClosed,
                   ChannelWrongStateError,
                   ConnectionClosed,
                   ConnectionWrongStateError,
                   StreamLostError),
    "delay": 1,
    "backoff": 2,
    "max_delay": 30,
    "logger": logger
}


class BaseConnector:
    """
    RabbitMQ connector that establishes a blocking connection and channel
    """

    def __init__(
        self,
        rabbitmq_host: str,
        rabbitmq_port: int,
        rabbitmq_user: str,
        rabbitmq_pass: str,
        retry_on_connection_error: bool,
        connection_name: str
    ):
        """
        Constructor for the base connector
        :param rabbitmq_host: Host name or IP of the rabbitMQ server. e.g. 127.0.0.1
        :param rabbitmq_port: Port the rabbitmq server listens on e.g. 5674
        :param rabbitmq_user: Username for the rabbitMQ server e.g. guest
        :param rabbitmq_pass: Password for the rabbitMQ server e.g. guest
        :param retry_on_connection_error: boolean to represent whether connection exceptions should be retried
        """
        self.connection_parameters = pika.ConnectionParameters(
            host=rabbitmq_host,
            port=rabbitmq_port,
            credentials=pika.credentials.PlainCredentials(rabbitmq_user, rabbitmq_pass),
            heartbeat=SECONDS_60,
            client_properties={"connection_name": connection_name}
        )
        if retry_on_connection_error:
            self.retries = RETRY_INDEFINITELY
        else:
            self.retries = RETRY_OFF
        self.connection = None
        self.channel = None

    @property
    def is_connected(self):
        """
        Current state of the connection.  Only updated when the connection is used.
        :return: Latest connection state
        """
        if self.connection:
            return self.connection.is_open
        return False

    def _connect(self):
        """
        Configures and initiates connection to the RabbitMQ server.
        :return:
        """
        logger.debug(f"Attempt to connect to RabbitMQ: connection_params={self.connection_parameters}")
        self.connection = pika.BlockingConnection(self.connection_parameters)
        logger.info(f"Connection Created")
        self.channel = self.connection.channel()
        logger.info("Channel Created")
        self.channel.confirm_delivery()  # ensure persistence prior to message confirmation
        self.channel.basic_qos(prefetch_count=1)  # only 1 un-Acked message delivered at a time
        logger.info("Channel configured")

    def connect(self):
        """
        Retries as configured the connection to the RabbitMQ server.
        :return:
        """
        retry_call(
            self._connect,
            tries=self.retries,
            **RABBITMQ_RETRY_CONFIG
        )

    def disconnect(self):
        """
        Closes connection and related channels to the RabbitMQ Server.
        :return:
        """
        self.connection.close()
        logger.warning(f"Disconnected from RabbitMQ: " f"connection={self.connection_parameters}")


class ConsumerConnector(BaseConnector):
    """
    RabbitMQ connector for consuming from a single queue in RabbitMQ.
    """

    def __init__(
        self,
        rabbitmq_host: str,
        rabbitmq_port: str,
        rabbitmq_user: str,
        rabbitmq_pass: str,
        consumer_queue: str,
        retry_on_connection_error: bool = True,
        connection_name: str = 'consumer connection'
    ):
        """
        Constructor for the consumer connector
        :param rabbitmq_host: Host name or IP of the rabbitMQ server. e.g. 127.0.0.1
        :param rabbitmq_port: Port the rabbitmq server listens on e.g. 5674
        :param rabbitmq_user: Username for the rabbitMQ server e.g. guest
        :param rabbitmq_pass: Password for the rabbitMQ server e.g. guest
        :param consumer_queue: Name of the queue the consumer will listen for messages on.
        :param retry_on_connection_error: boolean to represent whether connection exceptions should be retried
        """
        super().__init__(
            rabbitmq_host=rabbitmq_host,
            rabbitmq_port=rabbitmq_port,
            rabbitmq_user=rabbitmq_user,
            rabbitmq_pass=rabbitmq_pass,
            retry_on_connection_error=retry_on_connection_error,
            connection_name=connection_name
        )
        self.consumer_queue = consumer_queue

    def connect(self) -> None:
        """
        Configures and initiates connection to the RabbitMQ server
        :return:
        """
        super().connect()
        self.channel.queue_declare(queue=self.consumer_queue, durable=True)
        logger.info(f"Queue Created: queue={self.consumer_queue}")
        logger.success(f"Connected to RabbitMQ: " f"connection={self.connection_parameters}")

    def listen(self, callback: Callable) -> None:
        """
        Listens for messages on the channel configured on the consumer instance
        :param callback: Function to execute when a message is recieved. with the signature
        (ch, method, properties, body).
        ch: Copy of the channel used to acknowledge receipt (pika.Channel)
        method: Management keys for the delivered message e.g. delivery mode (pika.spec.Basic.Deliver)
        properties: Message properties (pika.spec.BasicProperties)
        body: Message body for a transfer message (bytes)
        :return: None
        """
        logger.info(f"Starting Listener on Queue: {self.consumer_queue}")
        self.channel.basic_consume(queue=self.consumer_queue, on_message_callback=callback)
        self.channel.start_consuming()


class ProducerConnector(BaseConnector):
    """
    RabbitMQ Connector for posting to 1 to many queues via an exchange
    """

    def __init__(
        self,
        producer_queue_bindings: List[dict],
        rabbitmq_host: str = "127.0.0.1",
        rabbitmq_port: int = 5674,
        rabbitmq_user: str = "guest",
        rabbitmq_pass: str = "guest",
        publish_exchange: str = "master.direct.x",
        retry_on_connection_error: bool = True,
        connection_name: str = "producer connection"
    ):
        """
        Constructor for the producer connector
        :param producer_queue_bindings: Bindings from routing key to destination queue for an exchange
        e.g. {"routing_key": "frame.audit.m", "bound_queue": "data.holding.audit.q"}
        :param rabbitmq_host: Host name or IP of the rabbitMQ server. e.g. 127.0.0.1
        :param rabbitmq_port: Port the rabbitmq server listens on e.g. 5674
        :param rabbitmq_user: Username for the rabbitMQ server e.g. guest
        :param rabbitmq_pass: Password for the rabbitMQ server e.g. guest
        :param publish_exchange: Name of the exchange that the  producer will publish to.
       :param retry_on_connection_error: boolean to represent whether connection exceptions should be retried
        """
        super().__init__(
            rabbitmq_host=rabbitmq_host,
            rabbitmq_port=rabbitmq_port,
            rabbitmq_user=rabbitmq_user,
            rabbitmq_pass=rabbitmq_pass,
            retry_on_connection_error=retry_on_connection_error,
            connection_name=connection_name
        )
        self.producer_queue_bindings = producer_queue_bindings
        self.publish_exchange = publish_exchange
        self.publish_message_properties = pika.BasicProperties(
            content_type="text/plain", priority=0, delivery_mode=2, content_encoding="UTF-8"
        )

    def connect(self) -> None:
        """
        Configures and initiates connection to the RabbitMQ server
        :return:
        """
        super().connect()
        for queue_binding in self.producer_queue_bindings:
            self.channel.queue_declare(queue=queue_binding["bound_queue"], durable=True)
            logger.info(f"Queue Created: queue={queue_binding['bound_queue']}")
            self.channel.queue_bind(
                exchange=self.publish_exchange,
                queue=queue_binding["bound_queue"],
                routing_key=queue_binding["routing_key"],
            )
            logger.info(
                f"Bindings configured: exchange={self.publish_exchange}, "
                f"queue={queue_binding['bound_queue']}, "
                f"routing_key={queue_binding['routing_key']} "
            )

        logger.success(f"Connected to RabbitMQ: " f"connection={self.connection_parameters}")

    def _validate_routing_key(self, routing_key: str):
        """
        Validate that the routing key is configured on the instance to prevent un-route-able posts
        :param routing_key: key to check for the existance of
        :return: True if the routing key exists on the instance. Raise a value error if it does not
        """
        if any([routing_key in binding["routing_key"] for binding in self.producer_queue_bindings]):
            return True

        raise ValueError(
            f"Routing key not configured: "
            f"routing_key={routing_key}, "
            f"initialized_routing_keys="
            f"{[binding['routing_key'] for binding in self.producer_queue_bindings]}"
        )

    def _post(self, routing_key: str, message: dict) -> None:
        """
        Post message to the exchange configured on the producer instance
        :param routing_key: routing key to use on the published message.  It must exist on the instance binding config
        :param message: body of the message to post
        :return: None
        """
        if not self.is_connected:
            self.connect()
        logger.debug(
            f"Attempt publish: "
            f"exchange={self.publish_exchange}, "
            f"routing_key={routing_key}, "
            f"body={json.dumps(message)},  "
            f"properties={self.publish_message_properties}, "
            f"mandatory=True"
        )
        self.channel.basic_publish(
            exchange=self.publish_exchange,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=self.publish_message_properties,
            mandatory=True,
        )

    def post(self, routing_key: str, message: dict) -> None:
        """
        Retry as configured  message post to the exchange configured on the producer instance
        :param routing_key: routing key to use on the published message.  It must exist on the instance binding config
        :param message: body of the message to post
        :return: None
        """

        self._validate_routing_key(routing_key)

        retry_call(
            self._post,
            fkwargs={"routing_key": routing_key, "message": message},
            tries=self.retries,
            **RABBITMQ_RETRY_CONFIG
        )
