from unittest.mock import ANY, Mock

import pytest

from great_expectations.agent.message_service.asyncio_rabbit_mq_client import (
    AsyncRabbitMQClient,
)
from great_expectations.agent.message_service.subscriber import (
    EventContext,
    Subscriber,
)
from tests.agent.message_service.amqp_errors import get_amqp_errors

pytestmark = pytest.mark.unit


def test_subscriber_consume_calls_run():
    client = Mock(autospec=AsyncRabbitMQClient)
    client.should_reconnect = False  # avoid infinite loop
    subscriber = Subscriber(client=client)
    queue = "test-queue"

    def on_message(event_context: EventContext) -> None:
        pass

    subscriber.consume(queue=queue, on_message=on_message)

    client.run.assert_called_with(queue=queue, on_message=ANY)


def test_subscriber_close_closes_channel():
    client = Mock(autospec=AsyncRabbitMQClient)
    subscriber = Subscriber(client=client)

    subscriber.close()

    client.stop.assert_called_with()


def test_subscriber_close_closes_connection():
    client = Mock(autospec=AsyncRabbitMQClient)
    subscriber = Subscriber(client=client)

    subscriber.close()

    client.stop.assert_called_with()


@pytest.mark.parametrize("error", get_amqp_errors())
def test_subscriber_close_handles_amqp_errors_from_channel(error):
    client = Mock(autospec=AsyncRabbitMQClient)
    client.channel.close.side_effect = error
    subscriber = Subscriber(client=client)

    subscriber.close()


@pytest.mark.parametrize("error", get_amqp_errors())
def test_subscriber_close_handles_amqp_errors_from_connection(error):
    client = Mock(autospec=AsyncRabbitMQClient)
    client.connection.close.side_effect = error
    subscriber = Subscriber(client=client)

    subscriber.close()  # no exception
