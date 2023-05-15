import os
from unittest.mock import patch

import pytest

from great_expectations.agent.agent import GXAgent, GXAgentConfig
from great_expectations.agent.message_service.rabbit_mq_client import ClientError
from great_expectations.agent.message_service.subscriber import SubscriberError


@pytest.fixture
def gx_agent_config(monkeypatch):
    config = GXAgentConfig(
        organization_id="92b8af7b-f89a-426a-8d79-1d451aea413b",
        broker_url="amqps://user:pass@great_expectations.io:5671",
    )
    env_vars = {
        "GE_CLOUD_ORGANIZATION_ID": config.organization_id,
        "BROKER_URL": config.broker_url,
    }
    monkeypatch.setattr(os, "environ", env_vars)
    return config


@pytest.fixture
def get_context():
    with patch("great_expectations.agent.agent.get_context") as get_context:
        yield get_context


@pytest.fixture
def client():
    """Patch for agent.RabbitMQClient"""
    with patch("great_expectations.agent.agent.RabbitMQClient") as client:
        yield client


@pytest.fixture
def subscriber():
    """Patch for agent.Subscriber"""
    with patch("great_expectations.agent.agent.Subscriber") as subscriber:
        yield subscriber


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
        queue=gx_agent_config.organization_id, on_message=agent._handle_event
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
