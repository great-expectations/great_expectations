from typing import TYPE_CHECKING, List

from great_expectations.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations.agent.models import (
    ListTableNamesEvent, CreatedResource,
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

        table_names_list_id: str = self._add_or_update_table_names_list(
            datasource_id=str(datasource.id),
            names_list=table_names
        )

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[
                CreatedResource(
                    resource_id=table_names_list_id,
                    type="TableNamesList"
                )
            ],
        )

    def _add_or_update_table_names_list(self, datasource_id: str, names_list: list[str]) -> str:
        # TODO: PUT to /datasources/<UUID>/table-names-list with body {"names_list": list[str]}
        # response returns id of TableNamesList
        pass
