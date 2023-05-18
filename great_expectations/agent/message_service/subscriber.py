from dataclasses import dataclass
from functools import partial
from typing import Callable, Union

import pydantic
from pika.adapters.blocking_connection import BlockingChannel
from pika.channel import Channel
from pika.exceptions import AMQPError, ChannelError
from pika.spec import Basic, BasicProperties

from great_expectations.agent.message_service.rabbit_mq_client import RabbitMQClient
from great_expectations.agent.models import Event


@dataclass(frozen=True)
class EventContext:
    """An Event with related properties and actions.

    Attributes:
        event: Pydantic model of type Event if parsable, None if not
        correlation_id: stable identifier for this Event over its lifecycle
        event_processed: callable with default parameter retry=False.
            Signals that the event has been processed, and exposes
            a path to attempt a retry.
    """

    event: Union[Event, None]
    correlation_id: str
    event_processed: Callable[[bool], None]


OnMessageCallback = Callable[[EventContext], None]


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
        channel: Channel,
        method_frame: Basic.Deliver,
        header_frame: BasicProperties,
        body: bytes,
        on_message: OnMessageCallback,
    ) -> None:
        """Called by Pika when a message is received.

        Translate message into a known model, obtain any required fields from headers,
        and pass results into on_message callback.

        Args:
            channel: the instance of pika.channel.Channel that delivered the message.
            method_frame: pika object containing context on the delivered message like delivery_tag,
                consumer_tag, redelivered, exchange, and routing_key.
            header_frame: pika object containing any header fields, such as correlation_id
            body: the message body in bytes
            on_message: the caller-provided callback
        """
        correlation_id = header_frame.correlation_id

        try:
            event: Event = pydantic.parse_raw_as(Event, body)  # type: ignore[arg-type]
        except pydantic.ValidationError:
            event = None

        event_context = EventContext(
            event=event,
            correlation_id=correlation_id,
            event_processed=partial(
                self._handle_event_processed,
                delivery_tag=method_frame.delivery_tag,
                _channel=channel,
            ),
        )

        return on_message(event_context)

    def _handle_event_processed(
        self, _delivery_tag: int, _channel: BlockingChannel, retry: bool = False
    ) -> None:
        """Callback passed to caller in EventContext.

        Allows the caller to request retry behavior based on their business logic.
        Args:
            _delivery_tag: private arg passed to the callback by the Subscriber
            _channel: private arg passed to the callback by the Subscriber
            retry: boolean indicating to retry processing this Event
        """
        try:
            if retry is True:
                _channel.basic_nack(delivery_tag=_delivery_tag)
            else:
                _channel.basic_ack(delivery_tag=_delivery_tag)
        except (AMQPError, ChannelError):
            pass  # if the _channel is no longer valid, we can't ack or nack this event

    def close(self) -> None:
        """Gracefully closes the Subscriber's connection.

        Must be called after the Subscriber disconnects."""
        try:
            self.client.channel.close()
        except (AMQPError, ChannelError):
            pass

        try:
            self.client.connection.close()
        except (AMQPError, ChannelError):
            pass


class SubscriberError(Exception):
    ...


class RequeueMessageError(Exception):
    """Message can't be processed and should be requeued"""

    ...
