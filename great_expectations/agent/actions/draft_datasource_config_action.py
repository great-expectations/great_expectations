from uuid import UUID

import pydantic

from great_expectations.agent.actions import ActionResult, AgentAction
from great_expectations.agent.config import GxAgentEnvVars
from great_expectations.agent.models import DraftDatasourceConfigEvent
from great_expectations.core.http import create_session


class DraftDatasourceConfigAction(AgentAction[DraftDatasourceConfigEvent]):
    def run(self, event: DraftDatasourceConfigEvent, id: str) -> ActionResult:
        draft_config = self.get_draft_config(config_id=event.config_id)
        datasource_type = draft_config.get("type", None)
        if datasource_type is None:
            raise ValueError(
                "The DraftDatasourceConfigAction can only be used with a fluent-style datasource."
            )
        try:
            datasource_cls = self._context.sources.type_lookup[datasource_type]
        except KeyError as exc:
            raise ValueError(
                "DraftDatasourceConfigAction received an unknown datasource type."
            ) from exc
        datasource = datasource_cls(**draft_config)
        datasource.test_connection(
            test_assets=True
        )  # raises `TestConnectionError` on failure
        return ActionResult(id=id, type=event.type, created_resources=[])

    def get_draft_config(self, config_id: UUID) -> dict:
        try:
            config = GxAgentEnvVars()
        except pydantic.ValidationError as validation_err:
            raise RuntimeError(
                f"Missing or badly formed environment variable\n{validation_err.errors()}"
            ) from validation_err
        resource_url = f"{config.gx_cloud_base_url}/organizations/{config.gx_cloud_organization_id}/draft-configs/{config_id}"
        session = create_session(access_token=config.gx_cloud_access_token)
        response = session.get(resource_url)
        if not response.ok:
            raise RuntimeError(
                "DraftDatasourceConfigAction encountered an error while connecting to GX-Cloud"
            )
        data = response.json()
        try:
            return data["data"]["attributes"]["draft_config"]
        except KeyError:
            raise RuntimeError("Malformed response received from GX-Cloud")
