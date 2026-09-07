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
from tests.integration.test_utils.data_source_config.sql import (
    DOUBLE_PRECISION_FLOAT_OVERRIDE,
    SQLBatchTestSetup,
)
from tests.integration.test_utils.data_source_config.sql_config import SqlDatasourceTestConfig


@register_sql_config
class SingleStoreDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="singlestore",
        public_name="SingleStore",
        marker="singlestore",
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"sql"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="singlestore"),
        uses_schema=False,
        tiers=frozenset({SupportTier.CURATED_SQL, SupportTier.FLUENT_API}),
        column_type_overrides={
            # SingleStore requires a length for VARCHAR, the same requirement MySQL declares.
            str: sqltypes.VARCHAR(255),
            # The shared default's `FLOAT(53)` reaches this server with its precision intact, and
            # unlike MySQL this server does not promote it: `SHOW CREATE TABLE` reports a plain
            # 4-byte `float`, and 16777217.0 comes back 16777200.0 -- valid DDL, no error. Naming
            # the 8-byte type round-trips the declared value.
            **DOUBLE_PRECISION_FLOAT_OVERRIDE,
        },
        dev_requirements_file="reqs/requirements-dev-singlestore.txt",
        task_runner_marker="singlestore",
        container_service="singlestore",
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
        return SingleStoreBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class SingleStoreBatchTestSetup(SQLBatchTestSetup[SingleStoreDatasourceTestConfig]):
    _BASE_CONNECTION_STRING = "singlestoredb://root:test_superuser@127.0.0.1:3306/test_ci"

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        return self._BASE_CONNECTION_STRING

    @override
    def make_asset(self) -> TableAsset:
        # No SingleStore-specific fluent datasource exists, so this reaches its datasource
        # through the dialect-agnostic SQL datasource instead.
        return self.context.data_sources.add_sql(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )
