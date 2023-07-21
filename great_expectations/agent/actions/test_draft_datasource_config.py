from great_expectations.agent.actions import ActionResult, AgentAction
from great_expectations.agent.models import TestDatasourceConfig


class TestDraftDatasourceConfigAction(AgentAction[TestDatasourceConfig]):
    def run(self, event: TestDatasourceConfig, id: str) -> ActionResult:
        # get config
        pass
