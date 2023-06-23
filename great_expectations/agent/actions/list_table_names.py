from sqlalchemy import inspect
from sqlalchemy.engine import Inspector

from great_expectations.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations.agent.models import (
    ListTableNamesEvent,
)
from great_expectations.datasource.fluent import Datasource as FluentDatasource, SQLDatasource


class ListTableNamesAction(AgentAction[ListTableNamesEvent]):
    def run(self, event: ListTableNamesEvent, id: str) -> ActionResult:
        datasource_name: str = event.datasource_name
        datasource: FluentDatasource = self._context.get_datasource(datasource_name=datasource_name)
        if not isinstance(datasource, SQLDatasource):
            raise TypeError(f"This operation requires a SQL Datasource but got {type(datasource).__name__}")

        inspector: Inspector = inspect(datasource.get_engine())
        table_names: list[str] = inspector.get_table_names()

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[],
            details=table_names
        )
