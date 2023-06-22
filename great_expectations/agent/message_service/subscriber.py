import asyncio
import time
from dataclasses import dataclass
from functools import partial
from json import JSONDecodeError
from typing import Callable, Coroutine, Union

import pydantic
from pika.exceptions import AMQPError, ChannelError

from great_expectations.agent.message_service.asyncio_rabbit_mq_client import (
    AsyncRabbitMQClient,
    OnMessagePayload,
)
from great_expectations.agent.models import Event, UnknownEvent


@dataclass(frozen=True)
class EventContext:
    """An Event with related properties and actions.

    Attributes:
        event: Pydantic model of type Event
        correlation_id: stable identifier for this Event over its lifecycle
        processed_successfully: callable to signal that the event was
            processed successfully and can be removed from the queue.
        processed_with_failures: callable to signal that processing failed and
            can be removed from the queue.
        redeliver_message: async callable to signal that the broker should
            try to deliver this message again.
    """

    event: Event
    correlation_id: str
    processed_successfully: Callable[[], None]
    processed_with_failures: Callable[[], None]
    redeliver_message: Callable[[], Coroutine]


OnMessageCallback = Callable[[EventContext], None]


class Subscriber:
    """Manage an open connection to an event stream."""

    # abstraction between the main application and client serving a specific stream

    def __init__(self, client: AsyncRabbitMQClient):
        """Initialize instance of Subscriber.

        Args:
            client: RabbitMQClient class.
        """
        self.client = client
        self._reconnect_delay = 0

    def consume(
        self,
        queue: str,
        on_message: OnMessageCallback,
    ) -> None:
        """Subscribe to queue with on_message callback.

        Listens to an event stream and invokes on_message with an EventContext
        built from the incoming message.

        Args:
            queue: Name of queue.
            on_message: Callback to be invoked with incoming messages.
        """
        callback = partial(self._on_message_handler, on_message=on_message)

        while True:
            try:
                self.client.run(queue=queue, on_message=callback)
            except (AMQPError, ChannelError):
                self.client.stop()
                reconnect_delay = self._get_reconnect_delay()
                time.sleep(
                    reconnect_delay
                )  # todo: update this blocking call to asyncio.sleep
            except KeyboardInterrupt as e:
                self.client.stop()
                raise KeyboardInterrupt from e
            if self.client.should_reconnect:
                self.client.reset()
            else:
                break  # exit

    def _on_message_handler(
        self,
        payload: OnMessagePayload,
        on_message: OnMessageCallback,
    ) -> None:
        """Called by the client when a message is received.

        Translate message into a known model and pass results into on_message callback.

        Args:
            payload: dataclass containing required message attributes
            on_message: the caller-provided callback
        """
        event: Event
        try:
            event = pydantic.parse_raw_as(Event, payload.body)  # type: ignore[arg-type]
        except (pydantic.ValidationError, JSONDecodeError):
            event = UnknownEvent()

        # Allow the caller to determine whether to ack/nack this message,
        # even if the processing occurs in another thread.
        ack_callback = self.client.get_threadsafe_ack_callback(
            delivery_tag=payload.delivery_tag
        )
        nack_callback = self.client.get_threadsafe_nack_callback(
            delivery_tag=payload.delivery_tag, requeue=False
        )
        # redeliver_message is not threadsafe
        redeliver_message = partial(
            self._redeliver_message,
            delivery_tag=payload.delivery_tag,
            requeue=True,
            delay=3,
        )

        event_context = EventContext(
            event=event,
            correlation_id=payload.correlation_id,
            processed_successfully=ack_callback,
            processed_with_failures=nack_callback,
            redeliver_message=redeliver_message,
        )

        return on_message(event_context)

    async def _redeliver_message(
        self, delivery_tag: int, requeue: bool = True, delay: Union[float, int] = 3
    ):
        """Coroutine to request a redelivery with delay."""
        # not threadsafe
        await asyncio.sleep(delay)
        return self.client.nack(delivery_tag=delivery_tag, requeue=requeue)

    def _get_reconnect_delay(self):
        """Get a timeout delay with a 1 second backoff for each attempt."""
        if self.client.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:  # noqa: PLR2004
            self._reconnect_delay = 30
        return self._reconnect_delay

    def close(self) -> None:
        """Gracefully closes the Subscriber's connection.

        Must be called after the Subscriber disconnects."""
        self.client.stop()


class SubscriberError(Exception):
    ...
