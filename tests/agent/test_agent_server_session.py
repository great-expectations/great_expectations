import pytest
from requests import Session as RequestsSession

from great_expectations.agent.agent_server_session import (
    AgentServerSession,
    GxAgentServerSessionConfig,
)


def test_server_session_session():
    config = GxAgentServerSessionConfig(
        gx_cloud_base_url="https://greatexpectations.io/test",
        gx_cloud_organization_id="fd7838b2-e29e-4594-b29f-6bd378284a43",
        gx_cloud_access_token="NGM3YTlkNGQtMWI3ZC00YTNhLTlhOTktMDQ5Y2I2YzAyNGE5",
    )

    server_session = AgentServerSession(config=config)

    session = server_session.session()

    assert isinstance(session, RequestsSession)


@pytest.mark.parametrize(
    "resource_endpoint, base_url",
    [
        # ensure leading/trailing slashes don't break url
        ("/test-resource", "https://greatexpectations.io/test"),
        ("/test-resource", "https://greatexpectations.io/test/"),
        ("test-resource", "https://greatexpectations.io/test"),
        ("test-resource", "https://greatexpectations.io/test/"),
    ],
)
def test_server_session_build_url(resource_endpoint, base_url):
    config = GxAgentServerSessionConfig(
        gx_cloud_base_url=base_url,
        gx_cloud_organization_id="fd7838b2-e29e-4594-b29f-6bd378284a43",
        gx_cloud_access_token="NGM3YTlkNGQtMWI3ZC00YTNhLTlhOTktMDQ5Y2I2YzAyNGE5",
    )
    server_session = AgentServerSession(config=config)

    url = server_session.build_url(resource_endpoint)

    assert (
        url
        == "https://greatexpectations.io/test/organizations/fd7838b2-e29e-4594-b29f-6bd378284a43/test-resource"
    )
