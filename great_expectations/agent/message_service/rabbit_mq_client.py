import ssl

import pika
from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError, ChannelError


class RabbitMQClient:
    """Configuration for a particular AMQP client library.

    Attributes:
        connection: an instance of pika.BlockingConnection
        channel: an instance of pika.adapters.blocking_connection.BlockingChannel
    """

    connection: BlockingConnection
    channel: BlockingChannel

    def __init__(self, url: str):
        try:
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")

            parameters = pika.URLParameters(url)
            parameters.ssl_options = pika.SSLOptions(context=ssl_context)
            self._parameters = parameters
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
        except (AMQPError, ChannelError) as e:
            raise ClientError from e

    def reset_connection(self):
        self.close()
        self.connection = pika.BlockingConnection(self._parameters)
        self.channel = self.connection.channel()

    def close(self):
        try:
            self.channel.close()
        except (AMQPError, ChannelError):
            pass

        try:
            self.connection.close()
        except (AMQPError, ChannelError):
            pass


class ClientError(Exception):
    ...
