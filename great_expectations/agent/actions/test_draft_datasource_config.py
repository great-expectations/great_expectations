from uuid import UUID

import pydantic

from great_expectations.agent.actions import ActionResult, AgentAction
from great_expectations.agent.agent import GxAgentConfigSettings
from great_expectations.agent.models import TestDatasourceConfig
from great_expectations.core.http import create_session


class TestDraftDatasourceConfigAction(AgentAction[TestDatasourceConfig]):
    def run(self, event: TestDatasourceConfig, id: str) -> ActionResult:
        draft_config = self.get_draft_config(config_id=event.config_id)
        datasource_type = draft_config.get("type", None)
        if datasource_type is None:
            raise ValueError("The TestDraftDatasourceConfigAction can only be used with a fluent-style datasource.")
        datasource_cls = self._context.sources.type_lookup[datasource_type]
        datasource = datasource_cls(**draft_config)
        datasource.test_connection()



    def get_draft_config(self, config_id: UUID) -> dict:
        return {"type": "pandas", "name": "test_123", "id": "4a6ec765-19ca-482c-a9fb-c17a8c869ccb"}
        # todo:
        # try:
        #     config = GxAgentConfigSettings()
        # except pydantic.ValidationError as validation_err:
        #     raise RuntimeError(
        #         f"Missing or badly formed environment variable\n{validation_err.errors()}"
        #     ) from validation_err
        # resource_url = f"{config.gx_cloud_base_url}/organizations/{config.gx_cloud_organization_id}/draft-configs/{config_id}"
        # session = create_session(access_token=config.gx_cloud_access_token)
        # response = session.get(resource_url)
        # if not response.ok:
        #     raise RuntimeError("TestDraftDatasourceConfigAction encountered an error while connecting to GX-Cloud")
        # data = response.json()
        # return data
