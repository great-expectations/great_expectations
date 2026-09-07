from typing import Mapping, Optional
from urllib.parse import urlencode

import pandas as pd
import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from tests.integration.sql_session_manager import SessionSQLEngineManager
from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec
from tests.integration.test_utils.data_source_config.base import BatchTestSetup
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    ExecutionEngineKind,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import register_sql_config
from tests.integration.test_utils.data_source_config.sql import SQLBatchTestSetup
from tests.integration.test_utils.data_source_config.sql_config import SqlDatasourceTestConfig


@register_sql_config
class PostgreSQLDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="postgresql",
        public_name="PostgreSQL",
        marker="postgresql",
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"postgres"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="postgresql"),
        uses_schema=True,
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        dev_requirements_file="reqs/requirements-dev-postgresql.txt",
        task_runner_marker="postgresql",
        container_service="postgresql",
    )

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        return PostgresBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class PostgresBatchTestSetup(SQLBatchTestSetup[PostgreSQLDatasourceTestConfig]):
    _BASE_CONNECTION_STRING = "postgresql+psycopg2://postgres@localhost:5432/test_ci"

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        if schema:
            options = urlencode({"options": f"-c search_path={schema}"})
            return f"{self._BASE_CONNECTION_STRING}?{options}"
        return self._BASE_CONNECTION_STRING

    @override
    def make_asset(self) -> TableAsset:
        return self.context.data_sources.add_postgres(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(schema=self.schema),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )
