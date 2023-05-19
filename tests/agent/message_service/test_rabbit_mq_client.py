import pytest

from great_expectations.agent.message_service.rabbit_mq_client import (
    ClientError,
    RabbitMQClient,
)
from tests.agent.message_service.amqp_errors import get_amqp_errors


@pytest.fixture
def pika(mocker):
    pika = mocker.patch(
        "great_expectations.agent.message_service.rabbit_mq_client.pika"
    )
    yield pika


def test_rabbit_mq_client_creates_connection(pika):
    """Expect that RabbitMQClient exposes a connection property, and that it
    is an instance of pika.BlockingConnection."""
    url = "test/url"

    client = RabbitMQClient(url=url)

    assert client.connection is pika.BlockingConnection()


def test_rabbit_mq_client_creates_channel(pika):
    """Expect that RabbitMQClient exposes a channel property, and that it is
    an instance of a BlockingChannel created from pika.BlockingConnection."""
    url = "test/url"

    client = RabbitMQClient(url=url)

    assert client.channel is pika.BlockingConnection().channel()


@pytest.mark.parametrize("error", get_amqp_errors())
def test_rabbit_mq_client_handles_amqp_error(error, pika):
    url = "test/url"
    pika.BlockingConnection.side_effect = error
    with pytest.raises(ClientError):
        RabbitMQClient(url=url)
