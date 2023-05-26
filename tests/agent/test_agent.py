import os

import pytest

from great_expectations.agent.agent import GXAgent, GXAgentConfig
from great_expectations.agent.message_service.asyncio_rabbit_mq_client import (
    ClientError,
)
from great_expectations.agent.message_service.subscriber import SubscriberError


@pytest.fixture(autouse=True)
def set_required_env_vars(monkeypatch):
    env_vars = {
        "GX_CLOUD_ORGANIZATION_ID": "4ea2985c-4fb7-4c53-9f8e-07b7e0506c3e",
        "GX_CLOUD_ACCESS_TOKEN": "MTg0NDkyYmYtNTBiOS00ZDc1LTk3MmMtYjQ0M2NhZDA2NjJk",
    }
    monkeypatch.setattr(os, "environ", env_vars)


@pytest.fixture
def gx_agent_config(queue, connection_string):
    config = GXAgentConfig(
        queue=queue,
        connection_string=connection_string,
    )
    return config


@pytest.fixture
def get_context(mocker):
    get_context = mocker.patch("great_expectations.agent.agent.get_context")
    return get_context


@pytest.fixture
def client(mocker):
    """Patch for agent.RabbitMQClient"""
    client = mocker.patch("great_expectations.agent.agent.AsyncRabbitMQClient")
    return client


@pytest.fixture
def subscriber(mocker):
    """Patch for agent.Subscriber"""
    subscriber = mocker.patch("great_expectations.agent.agent.Subscriber")
    return subscriber


@pytest.fixture
def queue():
    return "3ee9791c-4ea6-479d-9b05-98217e70d341"


@pytest.fixture
def connection_string():
    return "amqps://user:pass@great_expectations.io:5671"


@pytest.fixture(autouse=True)
def create_session(mocker, queue, connection_string):
    """Patch for great_expectations.core.http.create_session"""
    create_session = mocker.patch("great_expectations.agent.agent.create_session")
    create_session().post().json.return_value = {
        "queue": queue,
        "connection_string": connection_string,
    }
    return create_session


def test_gx_agent_gets_env_vars_on_init(get_context, gx_agent_config):
    agent = GXAgent()
    assert agent._config == gx_agent_config


def test_gx_agent_initializes_cloud_context(get_context, gx_agent_config):
    GXAgent()
    get_context.assert_called_with(cloud_mode=True)


def test_gx_agent_run_starts_subscriber(
    get_context, subscriber, client, gx_agent_config
):
    """Expect GXAgent.run to invoke the Subscriber class with the correct arguments."""
    agent = GXAgent()
    agent.run()

    subscriber.assert_called_with(client=client())


def test_gx_agent_run_invokes_consume(get_context, subscriber, client, gx_agent_config):
    """Expect GXAgent.run to invoke subscriber.consume with the correct arguments."""
    agent = GXAgent()
    agent.run()

    subscriber().consume.assert_called_with(
        queue=gx_agent_config.queue,
        on_message=agent._handle_event_as_thread_enter,
    )


def test_gx_agent_run_closes_subscriber(
    get_context, subscriber, client, gx_agent_config
):
    """Expect GXAgent.run to invoke subscriber.close."""
    agent = GXAgent()
    agent.run()

    subscriber().close.assert_called_with()


def test_gx_agent_run_handles_client_error_on_init(
    get_context, subscriber, client, gx_agent_config
):
    client.side_effect = ClientError
    agent = GXAgent()
    agent.run()


def test_gx_agent_run_handles_subscriber_error_on_init(
    get_context, subscriber, client, gx_agent_config
):
    subscriber.side_effect = SubscriberError
    agent = GXAgent()
    agent.run()


def test_gx_agent_run_handles_subscriber_error_on_consume(
    get_context, subscriber, client, gx_agent_config
):
    subscriber.consume.side_effect = SubscriberError
    agent = GXAgent()
    agent.run()


def test_gx_agent_run_handles_subscriber_error_on_close(
    get_context, subscriber, client, gx_agent_config
):
    subscriber.close.side_effect = SubscriberError
    agent = GXAgent()
    agent.run()
