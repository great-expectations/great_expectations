from unittest.mock import MagicMock
from uuid import UUID

import pytest

from great_expectations.agent.actions.test_draft_datasource_config import (
    TestDraftDatasourceConfigAction,
)
from great_expectations.agent.models import TestDatasourceConfig
from great_expectations.data_context import CloudDataContext


@pytest.fixture(scope="function")
def context():
    return MagicMock(autospec=CloudDataContext)


def test_test_draft_datasource_config_success(context):

    action = TestDraftDatasourceConfigAction(context=context)
    event = TestDatasourceConfig(config_id=UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5"))
    id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    action.run(event=event, id=str(id))
