from unittest.mock import ANY, Mock

import pytest

from great_expectations.agent.message_service.rabbit_mq_client import RabbitMQClient
from great_expectations.agent.message_service.subscriber import (
    EventContext,
    Subscriber,
)
from tests.agent.message_service.amqp_errors import get_amqp_errors


def test_subscriber_has_client_attribute():
    client = Mock(autospec=RabbitMQClient)

    subscriber = Subscriber(client=client)

    assert subscriber.client is client


def test_subscriber_consume_calls_basic_consume():
    client = Mock(autospec=RabbitMQClient)
    subscriber = Subscriber(client=client)
    queue = "test-queue"

    def on_message(event_context: EventContext) -> None:
        pass

    subscriber.consume(queue=queue, on_message=on_message, retries=1)

    client.channel.basic_consume.assert_called_with(
        queue=queue, on_message_callback=ANY
    )


def test_subscriber_consume_calls_start_consuming():
    client = Mock(autospec=RabbitMQClient)
    subscriber = Subscriber(client=client)
    queue = "test-queue"

    def on_message(event_context: EventContext) -> None:
        pass

    subscriber.consume(queue=queue, on_message=on_message, retries=1)

    client.channel.start_consuming.assert_called_with()


def test_subscriber_close_closes_channel():
    client = Mock(autospec=RabbitMQClient)
    subscriber = Subscriber(client=client)

    subscriber.close()

    client.close.assert_called_with()


def test_subscriber_close_closes_connection():
    client = Mock(autospec=RabbitMQClient)
    subscriber = Subscriber(client=client)

    subscriber.close()

    client.close.assert_called_with()


@pytest.mark.parametrize("error", get_amqp_errors())
def test_subscriber_close_handles_amqp_errors_from_channel(error):
    client = Mock(autospec=RabbitMQClient)
    client.channel.close.side_effect = error
    subscriber = Subscriber(client=client)

    subscriber.close()


@pytest.mark.parametrize("error", get_amqp_errors())
def test_subscriber_close_handles_amqp_errors_from_connection(error):
    client = Mock(autospec=RabbitMQClient)
    client.connection.close.side_effect = error
    subscriber = Subscriber(client=client)

    subscriber.close()  # no exception
