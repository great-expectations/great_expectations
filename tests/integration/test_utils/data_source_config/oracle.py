from datetime import datetime
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
class OracleDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="oracle",
        public_name="Oracle",
        marker="oracle",
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"sql"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="oracle"),
        # A schema in Oracle is a user, not a namespace the shared setup can create with a bare
        # `CREATE SCHEMA` statement: this dialect rejects that DDL with
        # `ORA-02420: missing schema authorization clause`, and there is no `DROP SCHEMA` to
        # tear one down either way. Isolation rides the harness's generated random table names
        # alone, the same shape SQLite, SingleStore, ClickHouse, and BigQuery already declare.
        uses_schema=False,
        # No transaction_mode override: Oracle has real transactions and the driver commits
        # explicitly, so the shared default (explicit commit) already matches its behavior.
        column_type_overrides={
            # Bare VARCHAR has no length; this dialect's DDL compiler rejects it with
            # `ORA-00906: missing left parenthesis`. SingleStore and MySQL declare the same
            # override for the same reason.
            str: sqltypes.VARCHAR(255),
            # The shared default's `DateTime` compiles to this dialect's DATE, which carries a
            # time of day but no fraction of a second. A declared microsecond is then dropped on
            # write, with valid DDL and no error: `2024-01-03 12:00:00.123456` comes back
            # `2024-01-03 12:00:00`. TIMESTAMP is this dialect's sub-second type and round-trips
            # the declared value intact, so the fixture frame is what the table holds.
            datetime: sqltypes.TIMESTAMP,
            pd.Timestamp: sqltypes.TIMESTAMP,
            # The shared default's precision-carrying FLOAT does not work here: this dialect
            # raises an argument error over binary vs. decimal precision. A DECIMAL carrying
            # explicit precision and scale round-trips fractional values intact, so that is
            # what is declared instead. Not a bare DECIMAL, which this dialect resolves to a
            # zero-scale NUMBER: the DDL succeeds, but 10.5 comes back 11.
            float: sqltypes.DECIMAL(38, 10),
        },
        dev_requirements_file="reqs/requirements-dev-oracle.txt",
        task_runner_marker="oracle",
        container_service="oracle",
        tiers=frozenset({SupportTier.CURATED_SQL, SupportTier.FLUENT_API}),
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
        return OracleBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class OracleBatchTestSetup(SQLBatchTestSetup[OracleDatasourceTestConfig]):
    # The driver-qualified scheme is required: a bare `oracle://` resolves to the legacy
    # thick-mode dialect, which tries to import a driver package this lane does not install.
    # `oracle+oracledb://` names the thin-mode driver explicitly. `service_name` (not SID)
    # addresses the container's pluggable database, matching the application user provisioned
    # there.
    _BASE_CONNECTION_STRING = (
        "oracle+oracledb://gx_test:test_app_pw@localhost:1521/?service_name=XEPDB1"
    )

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        # This backend declares no schema support, so `schema` is unused; the signature is the
        # shared abstract one (the ClickHouse comment pattern).
        return self._BASE_CONNECTION_STRING

    @override
    def make_asset(self) -> TableAsset:
        # No Oracle-specific fluent datasource exists, so this reaches its datasource through
        # the dialect-agnostic SQL datasource instead.
        return self.context.data_sources.add_sql(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )
