import pytest

from great_expectations.agent.message_service.asyncio_rabbit_mq_client import (
    AsyncRabbitMQClient,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def pika(mocker):
    pika = mocker.patch(
        "great_expectations.agent.message_service.asyncio_rabbit_mq_client.pika"
    )
    yield pika


@pytest.fixture
def asyncio_connection(mocker):
    asyncio_connection = mocker.patch(
        "great_expectations.agent.message_service.asyncio_rabbit_mq_client.AsyncioConnection"
    )
    yield asyncio_connection


def test_rabbit_mq_client_calls_run_forever(pika, asyncio_connection):
    url = "test/url"
    queue = "test-queue"

    def on_message():
        return None

    client = AsyncRabbitMQClient(url=url)

    client.run(queue=queue, on_message=on_message)

    asyncio_connection().ioloop.run_forever.assert_called_with()
