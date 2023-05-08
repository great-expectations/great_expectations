from functools import partial
from typing import Callable

import pydantic
from pika.exceptions import AMQPError, ChannelError

from great_expectations.agent.message_service.rabbit_mq_client import RabbitMQClient
from pika.channel import Channel
from pika.spec import Basic, BasicProperties
from pydantic import parse_raw_as

from great_expectations.agent.models import Event

OnMessageCallback = Callable[[Event, str], None]


class Subscriber:
    """Manage an open connection to an event stream."""

    def __init__(self, client: RabbitMQClient):
        """Initialize instance of Subscriber.

        Args:
            client: RabbitMQClient object with connection and channel attributes.
        """
        self.client = client

    def consume(self, queue: str, on_message: OnMessageCallback) -> None:
        """Subscribe to queue with on_message callback.

        Blocking call which listens to an event stream and invokes on_message
        with incoming messages.

        Args:
            queue: name of queue.
            on_message: callback to be invoked with incoming messages.
        """
        # avoid defining _callback_handler inline
        callback = partial(self._callback_handler, on_message=on_message)
        try:
            self.client.channel.basic_consume(queue=queue, on_message_callback=callback)
            self.client.channel.start_consuming()
        except (AMQPError, ChannelError) as e:
            raise SubscriberError from e
        except KeyboardInterrupt as e:
            self.client.channel.stop_consuming()
            raise KeyboardInterrupt from e
        # user is responsible for calling subscriber.close

    def _callback_handler(
        self,
        on_message: OnMessageCallback,
        channel: Channel,
        method_frame: Basic.Deliver,
        header_frame: BasicProperties,
        body: bytes,
    ) -> None:
        """Called by Pika when a message is received.

        Translate message into a known model, obtain any required fields from headers,
        and pass results into on_message callback.

        Args:
            on_message: the caller-provided callback
            channel: the instance of pika.channel.Channel that delivered the message.
            method_frame: pika object containing context on the delivered message like delivery_tag,
                consumer_tag, redelivered, exchange, and routing_key.
            header_frame: pika object containing any header fields, such as correlation_id
            body: the message body in bytes
        """
        try:
            event = parse_raw_as(Event, body)
            correlation_id = header_frame.correlation_id
            # we've successfully parsed the message, so ack to remove it from the queue
            # since we've acked, if our processing fails this message will be lost.
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            on_message(event, correlation_id)
        except pydantic.ValidationError:
            print("Received unknown message - doing nothing.")
            # we don't understand the message, so assume no one does.
            # in the future, this should nack unknown messages and
            # return them to the queue.
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def close(self):
        """Gracefully closes the Subscriber's connection.

        Must be called after the Subscriber disconnects."""
        try:
            self.client.channel.close()
            self.client.connection.close()
        except (AMQPError, ChannelError) as e:
            # if the channel and connection are already closed,
            # this might raise an exception
            raise SubscriberError from e


class SubscriberError(Exception):
    ...
