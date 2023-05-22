import ssl
from functools import partial
from typing import Callable

import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError, ChannelError
from pika.spec import Basic


class AsyncRabbitMQClient:
    """Configuration for a particular AMQP client library.

    Attributes:
        connection: an instance of pika.BlockingConnection
        channel: an instance of pika.adapters.blocking_connection.BlockingChannel
    """

    connection: AsyncioConnection
    channel: BlockingChannel

    def __init__(self, url: str):
        try:
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")

            parameters = pika.URLParameters(url)
            parameters.ssl_options = pika.SSLOptions(context=ssl_context)
            self._parameters = parameters

        except (AMQPError, ChannelError) as e:
            raise ClientError from e

        # from pika example
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = 1

    @property
    def connection(self):
        return self._connection

    @property
    def channel(self):
        return self._channel

    def run(self, queue: str, on_message: Callable):
        on_connection_callback = partial(
            self._on_connection_open, queue=queue, on_message=on_message
        )
        self._connection = AsyncioConnection(
            parameters=self._parameters,
            on_open_callback=on_connection_callback,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )
        self._connection.ioloop.run_forever()

    def _start_consuming(self, queue: str, on_message: Callable):
        self._channel.add_on_cancel_callback(self._on_consumer_canceled)
        self._consumer_tag = self._channel.basic_consume(
            queue=queue, on_message_callback=on_message
        )

    def _on_consumer_canceled(self, method_frame: Basic.Cancel):
        if self._channel:
            self._channel.close()

    def _reconnect(self):
        self.should_reconnect = True
        if not self._closing:
            self._closing = True
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.run_forever()
            else:
                self._connection.ioloop.stop()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            self._channel.basic_cancel(self._consumer_tag, callback=self._on_cancel_ok)

    def _on_cancel_ok(self, frame: Basic.CancelOk):
        self._consuming = False
        self._channel.close()

    def _on_connection_open(self, connection, queue: str, on_message: Callable):
        """Callback invoked after the broker opens the connection."""
        on_channel_open = partial(
            self._on_channel_open, queue=queue, on_message=on_message
        )
        connection.channel(on_open_callback=on_channel_open)

    def _on_connection_open_error(self, connection, reason):
        self._reconnect()

    def _on_connection_closed(self, connection, reason):
        """Callback invoked after the broker closes the connection"""
        self._channel = None
        if self._closing:
            connection.ioloop.stop()
        else:
            self._reconnect()

    def _close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            pass
        else:
            self._connection.close()

    def _on_channel_open(self, channel, queue: str, on_message: Callable):
        """Callback invoked after the broker opens the channel."""
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        self._start_consuming(queue=queue, on_message=on_message)

    def _on_channel_closed(self, channel, reason):
        """Callback invoked after the broker closes the channel."""
        self._close_connection()


class ClientError(Exception):
    ...
