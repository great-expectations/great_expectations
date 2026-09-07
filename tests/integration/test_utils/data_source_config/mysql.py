from typing import Mapping, Optional

import pandas as pd
import pytest

from great_expectations.compatibility.sqlalchemy import sqltypes
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
class MySQLDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="mysql",
        public_name="MySQL",
        marker="mysql",
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"sql"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="mysql"),
        uses_schema=True,
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        # MySQL requires a length for VARCHAR.
        column_type_overrides={str: sqltypes.VARCHAR(255)},
        dev_requirements_file="reqs/requirements-dev-mysql.txt",
        task_runner_marker="mysql",
        container_service="mysql",
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
        return MySQLBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class MySQLBatchTestSetup(SQLBatchTestSetup[MySQLDatasourceTestConfig]):
    _BASE_CONNECTION_STRING = "mysql+pymysql://root@localhost"

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        database = schema or "test_ci"
        return f"{self._BASE_CONNECTION_STRING}/{database}"

    @override
    def make_asset(self) -> TableAsset:
        return self.context.data_sources.add_sql(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(schema=self.schema),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )
