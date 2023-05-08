import ssl

import pika
from pika.exceptions import AMQPError, ChannelError


class RabbitMQClient:
    """Configuration for a particular AMQP client library.

    Attributes:
        connection: an instance of pika.BlockingConnection
        channel: an instance of pika.adapters.blocking_connection.BlockingChannel
    """

    def __init__(self, url: str):
        try:
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")

            parameters = pika.URLParameters(url)
            parameters.ssl_options = pika.SSLOptions(context=ssl_context)

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
        except (AMQPError, ChannelError) as e:
            raise ClientError from e


class ClientError(Exception):
    ...
