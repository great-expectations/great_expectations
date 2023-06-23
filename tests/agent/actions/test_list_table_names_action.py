from unittest import mock
from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import Inspector

from great_expectations.agent.actions import (
    ListTableNamesAction,
)
from great_expectations.agent.models import ListTableNamesEvent
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import (
    PandasDatasource,
    SQLDatasource,
)
from great_expectations.exceptions import StoreBackendError

pytestmark = pytest.mark.unit


@pytest.fixture(scope="function")
def context():
    return MagicMock(autospec=CloudDataContext)


@pytest.fixture
def event():
    return ListTableNamesEvent(
        type="list_table_names_request.received",
        datasource_name="test-datasource",
    )


def test_list_table_names_event_raises_for_non_sql_datasource(context, event):
    action = ListTableNamesAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    context.get_expectation_suite.side_effect = StoreBackendError("test-message")
    context.get_checkpoint.side_effect = StoreBackendError("test-message")
    datasource = MagicMock(spec=PandasDatasource)
    context.get_datasource.return_value = datasource

    with pytest.raises(
        TypeError, match=r"This operation requires a SQL Datasource but got"
    ):
        action.run(event=event, id=id)


def test_run_list_table_names_action_returns_action_result(context, event):
    action = ListTableNamesAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    with mock.patch(
        "great_expectations.agent.actions.list_table_names.inspect"
    ) as mock_inspect:
        datasource = MagicMock(spec=SQLDatasource)
        context.get_datasource.return_value = datasource

        table_names = ["table_1", "table_2", "table_3"]
        inspector = MagicMock(spec=Inspector)
        inspector.get_table_names.return_value = table_names

        mock_inspect.return_value = inspector

        action_result = action.run(event=event, id=id)

        assert action_result.type == event.type
        assert action_result.id == id
        assert action_result.details == table_names
