from __future__ import annotations

from typing import TYPE_CHECKING, Mapping, Optional
from urllib.parse import urlencode

from great_expectations.compatibility.pydantic import BaseSettings
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.redshift_datasource import RedshiftDsn
from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    ExecutionEngineKind,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import register_sql_config
from tests.integration.test_utils.data_source_config.sql import SQLBatchTestSetup
from tests.integration.test_utils.data_source_config.sql_config import SqlDatasourceTestConfig

if TYPE_CHECKING:
    import pandas as pd
    import pytest

    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.sql_datasource import TableAsset
    from tests.integration.sql_session_manager import SessionSQLEngineManager
    from tests.integration.test_utils.data_source_config.base import BatchTestSetup


class RedshiftConnectionConfig(BaseSettings):
    # BaseSettings will retrieve this environment variable
    REDSHIFT_DATABASE: str
    REDSHIFT_HOST: str
    REDSHIFT_PASSWORD: str
    REDSHIFT_PORT: int
    REDSHIFT_USERNAME: str
    REDSHIFT_SSLMODE: str

    def build_connection_string(self, schema: str | None = None) -> RedshiftDsn:
        options = f"&{urlencode({'options': f'-c search_path={schema}'})}" if schema else ""
        return RedshiftDsn(
            f"redshift+psycopg2://{self.REDSHIFT_USERNAME}:{self.REDSHIFT_PASSWORD}@"
            f"{self.REDSHIFT_HOST}:{self.REDSHIFT_PORT}/{self.REDSHIFT_DATABASE}?"
            f"sslmode={self.REDSHIFT_SSLMODE}{options}",
            scheme="redshift+psycopg2",
        )


@register_sql_config
class RedshiftDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="redshift",
        public_name="Redshift",
        marker="redshift",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"redshift"}),
        # Redshift's CI lane is a dedicated job rather than a `marker-tests` matrix entry, so
        # the job is named explicitly here rather than being the shared matrix job.
        ci_lane=CiLaneRef(workflow_job="redshift", marker_token="redshift"),
        uses_schema=True,
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        dev_requirements_file="reqs/requirements-dev-redshift.txt",
        task_runner_marker="redshift",
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
        return RedshiftBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class RedshiftBatchTestSetup(SQLBatchTestSetup[RedshiftDatasourceTestConfig]):
    @override
    def build_connection_string(self, schema: str | None = None) -> RedshiftDsn:
        return self.redshift_connection_config.build_connection_string(schema=schema)

    def __init__(
        self,
        config: RedshiftDatasourceTestConfig,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        table_name: Optional[str] = None,  # Overrides random table name generation
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> None:
        self.redshift_connection_config = RedshiftConnectionConfig()  # type: ignore[call-arg]  # retrieves env vars
        super().__init__(
            config=config,
            data=data,
            extra_data=extra_data,
            table_name=table_name,
            engine_manager=engine_manager,
            context=context,
        )

    @override
    def make_asset(self) -> TableAsset:
        return self.context.data_sources.add_redshift(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(schema=self.schema),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )
