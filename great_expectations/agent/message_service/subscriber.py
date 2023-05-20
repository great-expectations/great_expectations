from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from time import sleep
from typing import Callable, Optional, Union

import pydantic
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
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
        self._correlation_ids = defaultdict(lambda: 0)
        self._retries = 0

    def consume(
        self,
        queue: str,
        on_message: OnMessageCallback,
        retry_limit: Optional[int] = None,
        retry_delay: Union[int, float] = 0.5,
    ) -> None:

        """Subscribe to queue with on_message callback.

        Blocking call which listens to an event stream and invokes on_message
        with incoming messages.

        Args:
            queue: Name of queue.
            on_message: Callback to be invoked with incoming messages.
            retry_limit: Optionally, maximum number of reconnects over the
                lifetime of this consume. Defaults to None.
            retry_delay: Number of seconds to wait before retrying on failed connection.
        """
        # avoid defining _callback_handler inline
        callback = partial(self._callback_handler, on_message=on_message)

        while self._should_retry(max=retry_limit):
            try:
                self.client.channel.basic_consume(
                    queue=queue, on_message_callback=callback
                )
                self.client.channel.start_consuming()
            except (AMQPError, ChannelError) as e:
                print("Error in connection to GX Cloud - retrying.")
                print(e)
                self.client.reset_connection()
            except KeyboardInterrupt as e:
                self.client.channel.stop_consuming()
                raise KeyboardInterrupt from e
            sleep(retry_delay)

        print("Unable to connect - please check your network and restart the agent.")
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
        delivery_tag = method_frame.delivery_tag
        if self._reject_correlation_id(id=correlation_id) is True:
            # we've seen this message too many times, request that it's removed from the queue
            print(f"Removing job {correlation_id} because it failed too many times.")
            return channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
        try:
            event: Event = pydantic.parse_raw_as(Event, body)  # type: ignore[arg-type]
        except pydantic.ValidationError:
            event = None

        # this callback allows the caller to determine whether the message
        # should be acked or nacked without knowing implementation details.
        event_processed_callback = partial(
            self._handle_event_processed,
            _delivery_tag=method_frame.delivery_tag,
            _channel=channel,
            _connection=self.client.connection,
        )

        event_context = EventContext(
            event=event,
            correlation_id=correlation_id,
            event_processed=event_processed_callback,
        )

        return on_message(event_context)

    def _handle_event_processed(
        self,
        _delivery_tag: int,
        _channel: BlockingChannel,
        _connection: BlockingConnection,
        retry: bool = False,
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
                self._ack(
                    connection=_connection, channel=_channel, delivery_tag=_delivery_tag
                )
            else:
                self._nack(
                    connection=_connection,
                    channel=_channel,
                    delivery_tag=_delivery_tag,
                    retry=True,
                )
        except (AMQPError, ChannelError):
            # if the _channel is no longer valid, we can't ack or nack this event
            # best we can do is reset the connection and try again
            pass

    def _ack(
        self,
        connection: BlockingConnection,
        channel: BlockingChannel,
        delivery_tag: int,
    ) -> None:
        """Ack a message in a threadsafe manner."""
        if channel.is_closed is not True:
            ack = partial(channel.basic_ack, delivery_tag=delivery_tag)
            connection.add_callback_threadsafe(callback=ack)
        else:
            pass  # ship has sailed

    def _nack(
        self,
        connection: BlockingConnection,
        channel: BlockingChannel,
        delivery_tag: int,
        retry: bool = True,
    ) -> None:
        """Nack a message in a threadsafe manner, and indicate if it should be redelivered."""
        if channel.is_closed is not True:
            nack = partial(channel.basic_nack, delivery_tag=delivery_tag, requeue=retry)
            connection.add_callback_threadsafe(callback=nack)
        else:
            pass  # ship has sailed

    def _reject_correlation_id(self, id: str):
        """Has this correlation ID been seen too many times?"""
        MAX_REDELIVERY = 10
        self._correlation_ids[id] += 1
        delivery_count = self._correlation_ids[id]
        if delivery_count > MAX_REDELIVERY:
            return True
        else:
            return False

    def _should_retry(self, max: Optional[int] = None) -> bool:
        """Returns True the first max times this method is called, then False."""
        self._retries += 1
        if max is None:
            return True
        elif self._retries <= max:
            return True
        else:
            return False

    def close(self) -> None:
        """Gracefully closes the Subscriber's connection.

        Must be called after the Subscriber disconnects."""
        self.client.close()


class SubscriberError(Exception):
    ...
