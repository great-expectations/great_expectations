from uuid import UUID

from great_expectations import get_context
from great_expectations.agent.actions.test_draft_datasource_config import TestDraftDatasourceConfigAction
from great_expectations.agent.models import TestDatasourceConfig


def test_test_draft_datasource_config_success():
    context = get_context(cloud_mode=True)
    action = TestDraftDatasourceConfigAction(context=context)
    event = TestDatasourceConfig(
        config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    )
    id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    action.run(event=event, id=str(id))
    print()

