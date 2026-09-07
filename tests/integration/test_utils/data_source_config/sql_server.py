import logging
from typing import Mapping, Optional

import pandas as pd
import pytest

from great_expectations.compatibility.sqlalchemy import TextClause, create_engine
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from great_expectations.datasource.fluent.sql_server_datasource import (
    SQLServerAuthConnectionDetails,
)
from tests.integration.sql_session_manager import ConnectionDetails, SessionSQLEngineManager
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
from tests.test_utils import (
    SQL_SERVER_DATABASE,
    SQL_SERVER_DRIVER,
    SQL_SERVER_ENCRYPT,
    SQL_SERVER_HOST,
    SQL_SERVER_PASSWORD,
    SQL_SERVER_PORT,
    SQL_SERVER_SCHEMA,
    SQL_SERVER_USERNAME,
)

logger = logging.getLogger(__name__)


@register_sql_config
class SQLServerDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="mssql",
        public_name="SQL Server",
        marker="sql_server",
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"sql_server"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="sql_server"),
        uses_schema=True,
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        dev_requirements_file="reqs/requirements-dev-sql-server.txt",
        task_runner_marker="sql_server",
        container_service="mssql",
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
        return SQLServerBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class SQLServerBatchTestSetup(SQLBatchTestSetup[SQLServerDatasourceTestConfig]):
    def _connection_details(self, schema: str | None = None) -> SQLServerAuthConnectionDetails:
        return SQLServerAuthConnectionDetails(
            host=SQL_SERVER_HOST,
            port=SQL_SERVER_PORT,
            database=SQL_SERVER_DATABASE,
            schema=schema or SQL_SERVER_SCHEMA,
            username=SQL_SERVER_USERNAME,
            password=SQL_SERVER_PASSWORD,
            driver=SQL_SERVER_DRIVER,
            encrypt=SQL_SERVER_ENCRYPT,
            trust_server_certificate=True,
        )

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        # autocommit prevents implicit transactions from holding schema locks
        # that block DDL (CREATE/DROP SCHEMA, CREATE/DROP TABLE)
        url = self._connection_details(schema).build_connection_string()
        return f"{url}&autocommit=true"

    @override
    def make_asset(self) -> TableAsset:
        return self.context.data_sources.add_sql_server(
            name=self._random_resource_name(),
            connection_string=self._connection_details(schema=self.schema),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )

    def dispose_connections_for_teardown(self) -> None:
        """Close/dispose SQL Server engines before schema teardown."""
        for datasource in self.context.data_sources.all().values():
            execution_engine = datasource.execution_engine
            if execution_engine:
                execution_engine.close()
            if datasource._engine:
                datasource._engine.dispose()
                datasource._engine = None

        if self.engine_manager:
            self.engine_manager.dispose_engine(
                ConnectionDetails(connection_string=self.build_connection_string())
            )

    @override
    def teardown(self) -> None:
        """Override teardown to dispose engines and fail fast on lock waits."""
        self.dispose_connections_for_teardown()

        engine = create_engine(url=self.build_connection_string())
        try:
            with engine.connect() as conn:
                conn.execute(TextClause("SET LOCK_TIMEOUT 30000"))
                for table in self.tables:
                    table.drop(conn)
                if self.schema:
                    logger.info(f"DROPPING SCHEMA {self.schema}")
                    try:
                        conn.execute(TextClause(f"DROP SCHEMA {self.schema}"))
                    except Exception as err:
                        # Best-effort cleanup: SQL Server can intermittently keep schema
                        # locks after tests complete. Avoid failing tests on teardown-only
                        # lock contention in ephemeral CI containers.
                        if "Lock request time out period exceeded" in str(err) and "(1222)" in str(
                            err
                        ):
                            logger.warning(
                                f"Skipping DROP SCHEMA for {self.schema} due to lock timeout: {err}"
                            )
                        else:
                            raise
        finally:
            engine.dispose()
