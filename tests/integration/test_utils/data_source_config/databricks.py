from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Mapping, Optional, Type, Union

from great_expectations.compatibility.pydantic import BaseSettings
from great_expectations.compatibility.sqlalchemy import TypeEngine, sqltypes
from great_expectations.compatibility.typing_extensions import override
from tests.integration.test_utils.data_source_config.backend_spec import (
    SqlBackendSpec,
    TransactionMode,
)
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    ExecutionEngineKind,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import register_sql_config
from tests.integration.test_utils.data_source_config.sql import (
    DOUBLE_PRECISION_FLOAT_OVERRIDE,
    SQLBatchTestSetup,
)
from tests.integration.test_utils.data_source_config.sql_config import SqlDatasourceTestConfig

if TYPE_CHECKING:
    import pandas as pd
    import pytest

    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.sql_datasource import TableAsset
    from tests.integration.sql_session_manager import SessionSQLEngineManager
    from tests.integration.test_utils.data_source_config.base import BatchTestSetup


# The shared default's precision is discarded by this dialect, and the bare `FLOAT` it leaves is
# this server's 4-byte type, so the declared override names the 8-byte one instead.
_COLUMN_TYPE_OVERRIDES: Mapping[type, Union[Type[TypeEngine], TypeEngine]] = {
    # databricks requires a length for VARCHAR
    str: sqltypes.VARCHAR(255),
    **DOUBLE_PRECISION_FLOAT_OVERRIDE,
}


@register_sql_config
class DatabricksDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="databricks",
        public_name="Databricks (SQL)",
        marker="databricks",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"databricks_sql"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="databricks"),
        uses_schema=True,
        transaction_mode=TransactionMode.AUTOCOMMIT,
        column_type_overrides=_COLUMN_TYPE_OVERRIDES,
        insert_parameter_limit=250,
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        dev_requirements_file="reqs/requirements-dev-databricks.txt",
        task_runner_marker="databricks",
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
        return DatabricksBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class DatabricksBatchTestSetup(SQLBatchTestSetup[DatabricksDatasourceTestConfig]):
    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        return self._databrics_connection_config.build_connection_string(schema=schema)

    @cached_property
    def _databrics_connection_config(self) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig()  # type: ignore[call-arg]  # retrieves env vars

    @override
    def make_asset(self) -> TableAsset:
        assert self.schema
        return self.context.data_sources.add_databricks_sql(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(schema=self.schema),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )


class DatabricksConnectionConfig(BaseSettings):
    databricks_token: str
    databricks_host: str
    databricks_http_path: str
    databricks_catalog: str

    def build_connection_string(self, schema: str | None = None) -> str:
        base = (
            "databricks://token:"
            f"{self.databricks_token}@{self.databricks_host}:443"
            f"?http_path={self.databricks_http_path}&catalog={self.databricks_catalog}"
        )
        if schema:
            return f"{base}&schema={schema}"
        return base
