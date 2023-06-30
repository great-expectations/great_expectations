from typing import TYPE_CHECKING, List

from great_expectations.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations.agent.models import (
    ListTableNamesEvent,
)
from great_expectations.compatibility.sqlalchemy import inspect
from great_expectations.datasource.fluent import Datasource as FluentDatasource
from great_expectations.datasource.fluent import SQLDatasource

if TYPE_CHECKING:
    from great_expectations.compatibility.sqlalchemy.engine import Inspector


class ListTableNamesAction(AgentAction[ListTableNamesEvent]):
    def run(self, event: ListTableNamesEvent, id: str) -> ActionResult:
        datasource_name: str = event.datasource_name
        datasource: FluentDatasource = self._context.get_datasource(
            datasource_name=datasource_name
        )
        if not isinstance(datasource, SQLDatasource):
            raise TypeError(
                f"This operation requires a SQL Datasource but got {type(datasource).__name__}."
            )

        inspector: Inspector = inspect(datasource.get_engine())
        table_names: List[str] = inspector.get_table_names()

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[],
        )
