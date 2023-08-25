from typing import TYPE_CHECKING, List

import pydantic

from great_expectations.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations.agent.config import GxAgentEnvVars
from great_expectations.agent.models import (
    ListTableNamesEvent,
)
from great_expectations.compatibility.sqlalchemy import inspect
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.http import create_session
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.exceptions import GXCloudError

if TYPE_CHECKING:
    from great_expectations.compatibility.sqlalchemy.engine import Inspector


class ListTableNamesAction(AgentAction[ListTableNamesEvent]):
    @override
    def run(self, event: ListTableNamesEvent, id: str) -> ActionResult:
        datasource_name: str = event.datasource_name
        datasource = self._context.get_datasource(datasource_name=datasource_name)
        if not isinstance(datasource, SQLDatasource):
            raise TypeError(
                f"This operation requires a SQL Datasource but got {type(datasource).__name__}."
            )

        inspector: Inspector = inspect(datasource.get_engine())
        table_names: List[str] = inspector.get_table_names()

        self._add_or_update_table_names_list(
            datasource_id=str(datasource.id), table_names=table_names
        )

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[],
        )

    def _add_or_update_table_names_list(
        self, datasource_id: str, table_names: List[str]
    ) -> None:
        try:
            cloud_config = GxAgentEnvVars()
        except pydantic.ValidationError as validation_err:
            raise RuntimeError(
                f"Missing or badly formed environment variable\n{validation_err.errors()}"
            ) from validation_err

        session = create_session(access_token=cloud_config.gx_cloud_access_token)
        response = session.patch(
            url=f"{cloud_config.gx_cloud_base_url}/organizations/"
            f"{cloud_config.gx_cloud_organization_id}/datasources/{datasource_id}",
            json={"table_names": table_names},
        )
        if response.status_code != 204:  # noqa: PLR2004
            raise GXCloudError(
                message=f"ListTableNamesAction encountered an error while connecting to GX Cloud. Unable to update "
                f"table_names for Datasource with id"
                f"={datasource_id}.",
                response=response,
            )
