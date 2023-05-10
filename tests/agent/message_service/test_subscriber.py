from unittest.mock import Mock, ANY

import pytest

from great_expectations.agent.message_service.rabbit_mq_client import RabbitMQClient
from great_expectations.agent.message_service.subscriber import (
    Subscriber,
    SubscriberError,
)
from great_expectations.agent.models import Event
from tests.agent.message_service.conftest import AMQP_CHANNEL_AND_CONNECTION_ERRORS


def test_subscriber_has_client_attribute():
    client = Mock(autospec=RabbitMQClient)

    subscriber = Subscriber(client=client)

    assert subscriber.client is client


def test_subscriber_consume_calls_basic_consume():
    client = Mock(autospec=RabbitMQClient)
    subscriber = Subscriber(client=client)
    queue = "test-queue"

    def on_message(event: Event, correlation_id: str) -> None:
        pass

    subscriber.consume(queue=queue, on_message=on_message)

    client.channel.basic_consume.assert_called_with(
        queue=queue, on_message_callback=ANY
    )


def test_subscriber_consume_calls_start_consuming():
    client = Mock(autospec=RabbitMQClient)
    subscriber = Subscriber(client=client)
    queue = "test-queue"

    def on_message(event: Event, correlation_id: str) -> None:
        pass

    subscriber.consume(queue=queue, on_message=on_message)

    client.channel.start_consuming.assert_called_with()


def test_subscriber_close_closes_channel():
    client = Mock(autospec=RabbitMQClient)
    subscriber = Subscriber(client=client)

    subscriber.close()

    client.channel.close.assert_called_with()


def test_subscriber_close_closes_connection():
    client = Mock(autospec=RabbitMQClient)
    subscriber = Subscriber(client=client)

    subscriber.close()

    client.connection.close.assert_called_with()


@pytest.mark.parametrize("error", AMQP_CHANNEL_AND_CONNECTION_ERRORS)
def test_subscriber_close_handles_amqp_errors_from_channel(error):
    client = Mock(autospec=RabbitMQClient)
    client.channel.close.side_effect = error
    subscriber = Subscriber(client=client)

    with pytest.raises(SubscriberError):
        subscriber.close()


@pytest.mark.parametrize("error", AMQP_CHANNEL_AND_CONNECTION_ERRORS)
def test_subscriber_close_handles_amqp_errors_from_connection(error):
    client = Mock(autospec=RabbitMQClient)
    client.connection.close.side_effect = error
    subscriber = Subscriber(client=client)

    with pytest.raises(SubscriberError):
        subscriber.close()
