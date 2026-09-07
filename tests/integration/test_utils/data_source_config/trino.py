from typing import Mapping, Optional

import pandas as pd
import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from tests.integration.sql_session_manager import SessionSQLEngineManager
from tests.integration.test_utils.data_source_config.backend_spec import (
    SqlBackendSpec,
    TransactionMode,
)
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
class TrinoDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="trino",
        public_name="Trino",
        marker="trino",
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"sql"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="trino"),
        uses_schema=True,
        transaction_mode=TransactionMode.AUTOCOMMIT,
        tiers=frozenset({SupportTier.CURATED_SQL, SupportTier.FLUENT_API}),
        dev_requirements_file="reqs/requirements-dev-trino.txt",
        task_runner_marker="trino",
        container_service="trino",
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
        return TrinoBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class TrinoBatchTestSetup(SQLBatchTestSetup[TrinoDatasourceTestConfig]):
    _CATALOG_PREFIX = "trino://test@localhost:8088/memory"
    _DEFAULT_SCHEMA = "default"

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        # The memory connector always has a `default` schema, so setup/teardown (which
        # connect with no schema) always have a valid session schema to run DDL against.
        return f"{self._CATALOG_PREFIX}/{schema or self._DEFAULT_SCHEMA}"

    @override
    def make_asset(self) -> TableAsset:
        # No Trino-specific fluent datasource exists, so this reaches its datasource through
        # the dialect-agnostic SQL datasource instead.
        return self.context.data_sources.add_sql(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(schema=self.schema),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )
