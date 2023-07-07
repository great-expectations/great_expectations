from requests import Session

from great_expectations.agent.agent import GXAgentConfig
from great_expectations.core.http import create_session


class AgentServerSession:
    def __init__(self, config: GXAgentConfig):
        self._config = config

    def session(self) -> Session:
        return create_session(access_token=self._config.gx_cloud_access_token)

    def build_url(self, resource_endpoint: str) -> str:
        """Construct a fully qualified URL."""
        if not resource_endpoint.startswith("/"):
            resource_endpoint = "/" + resource_endpoint
        base_url = self._config.gx_cloud_base_url
        if base_url.endswith("/"):
            base_url = base_url[:-1]
        return f"{base_url}/organizations/{self._config.gx_cloud_organization_id}{resource_endpoint}"
