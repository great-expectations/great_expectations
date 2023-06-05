import asyncio
import ssl
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from functools import partial
from typing import Callable

import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.channel import Channel
from pika.spec import Basic, BasicProperties


@dataclass(frozen=True)
class OnMessagePayload:
    correlation_id: str
    delivery_tag: int
    body: bytes


class AsyncRabbitMQClient:
    """Configuration for a particular AMQP client library."""

    def __init__(self, url: str):
        self._parameters = self._build_client_parameters(url=url)
        self.should_reconnect = False
        self.was_consuming = False
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False

    def run(self, queue: str, on_message: Callable[[OnMessagePayload], None]) -> None:
        """Run an async connection to RabbitMQ.

        Args:
            queue: string representing queue to subscribe to
            on_message: callback which will be invoked when a message is received.
        """
        # When Pika receives an incoming message, our method _callback_handler will
        # be invoked. Here we add the user-provided callback to its signature:
        on_message_callback = partial(self._callback_handler, on_message=on_message)
        # We pass the curried on_message_callback and queue name to _on_connection_open,
        # which Pika will invoke after RabbitMQ establishes our connection.
        on_connection_open_callback = partial(
            self._on_connection_open, queue=queue, on_message=on_message_callback
        )
        connection = AsyncioConnection(
            parameters=self._parameters,
            on_open_callback=on_connection_open_callback,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )
        self._connection = connection
        connection.ioloop.run_forever()

    def stop(self) -> None:
        """Close the connection to RabbitMQ."""
        if self._connection is None:
            return
        if not self._closing:
            self._closing = True
            if self._consuming:
                self._stop_consuming()
                self._connection.ioloop.run_forever()
            else:
                self._connection.ioloop.stop()

    def reset(self) -> None:
        """Reset client to allow a restart."""
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False

    def nack(self, delivery_tag: int, requeue: bool) -> None:
        """Nack a message, and indicate if it should be requeued.

        Note that this method is not threadsafe, and must be invoked in the main thread.
        """
        if self._channel is not None:
            self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def get_threadsafe_ack_callback(self, delivery_tag: int) -> Callable[[], None]:
        """Get a callback to ack a message from any thread."""
        if self._channel is None:
            # if the channel is gone, we can't ack
            return lambda: None
        loop = asyncio.get_event_loop()
        ack = partial(
            self._ack_threadsafe,
            channel=self._channel,
            loop=loop,
            delivery_tag=delivery_tag,
        )
        return ack

    def get_threadsafe_nack_callback(
        self, delivery_tag: int, requeue: bool = False
    ) -> Callable[[], None]:
        """Get a callback to nack a message from any thread."""
        if self._channel is None:
            # if the channel is gone, we can't nack
            return lambda: None
        loop = asyncio.get_event_loop()
        nack = partial(
            self._nack_threadsafe,
            channel=self._channel,
            loop=loop,
            delivery_tag=delivery_tag,
            requeue=requeue,
        )
        return nack

    def _ack_threadsafe(
        self, channel: Channel, delivery_tag: int, loop: AbstractEventLoop
    ):
        """Ack a message in a threadsafe manner."""
        if channel.is_closed is not True:
            ack = partial(channel.basic_ack, delivery_tag=delivery_tag)
            loop.call_soon_threadsafe(callback=ack)

    def _nack_threadsafe(
        self,
        channel: Channel,
        delivery_tag: int,
        loop: AbstractEventLoop,
        requeue: bool,
    ):
        """Nack a message in a threadsafe manner."""
        if channel.is_closed is not True:
            nack = partial(
                channel.basic_nack, delivery_tag=delivery_tag, requeue=requeue
            )
            loop.call_soon_threadsafe(callback=nack)

    def _callback_handler(  # noqa: PLR0913
        self,
        channel: Channel,
        method_frame: Basic.Deliver,
        header_frame: BasicProperties,
        body: bytes,
        on_message: Callable[[OnMessagePayload], None],
    ) -> None:
        """Called by Pika when a message is received."""
        # param on_message is provided by the caller as an argument to AsyncRabbitMQClient.run
        correlation_id = header_frame.correlation_id
        delivery_tag = method_frame.delivery_tag
        payload = OnMessagePayload(
            correlation_id=correlation_id, delivery_tag=delivery_tag, body=body
        )
        return on_message(payload)

    def _start_consuming(self, queue: str, on_message: Callable, channel: Channel):
        """Consume from a channel with the on_message callback."""
        channel.add_on_cancel_callback(self._on_consumer_canceled)
        self._consumer_tag = channel.basic_consume(
            queue=queue, on_message_callback=on_message
        )

    def _on_consumer_canceled(self, method_frame: Basic.Cancel):
        """Callback invoked when the broker cancels the client's connection."""
        if self._channel is not None:
            self._channel.close()

    def _reconnect(self):
        """Prepare the client to reconnect."""
        self.should_reconnect = True
        self.stop()

    def _stop_consuming(self):
        """Cancel the channel, if it exists."""
        if self._channel is not None:
            self._channel.basic_cancel(self._consumer_tag, callback=self._on_cancel_ok)

    def _on_cancel_ok(self, method_frame: Basic.CancelOk):
        """Callback invoked after broker confirms cancel."""
        self._consuming = False
        if self._channel is not None:
            self._channel.close()

    def _on_connection_open(
        self, connection: AsyncioConnection, queue: str, on_message: Callable
    ):
        """Callback invoked after the broker opens the connection."""
        on_channel_open = partial(
            self._on_channel_open, queue=queue, on_message=on_message
        )
        connection.channel(on_open_callback=on_channel_open)

    def _on_connection_open_error(self, connection, reason):
        """Callback invoked when there is an error while opening connection."""
        self._reconnect()

    def _on_connection_closed(self, connection, reason):
        """Callback invoked after the broker closes the connection"""
        self._channel = None
        if self._closing:
            connection.ioloop.stop()
        else:
            self._reconnect()

    def _close_connection(self):
        """Close the connection to the broker."""
        self._consuming = False
        if (
            self._connection is None
            or self._connection.is_closing
            or self._connection.is_closed
        ):
            pass
        else:
            self._connection.close()

    def _on_channel_open(self, channel: Channel, queue: str, on_message: Callable):
        """Callback invoked after the broker opens the channel."""
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_closed)
        self._start_consuming(queue=queue, on_message=on_message, channel=channel)

    def _on_channel_closed(self, channel, reason):
        """Callback invoked after the broker closes the channel."""
        self._close_connection()

    def _build_client_parameters(self, url: str):
        """Configure parameters used to connect to the broker."""
        parameters = pika.URLParameters(url)
        # only enable SSL if connection string calls for it
        if url.startswith("amqps://"):
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")
            parameters.ssl_options = pika.SSLOptions(context=ssl_context)
        return parameters


class ClientError(Exception):
    ...
