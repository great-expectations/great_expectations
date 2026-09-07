import pathlib
from typing import Mapping, Optional

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
class SqliteDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="sqlite",
        public_name="SQLite",
        marker="sqlite",
        provisioning=DataSourceProvisioning.LOCAL_FILE,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"sqlite"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="sqlite"),
        uses_schema=False,
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        # SQLite has neither a dev-requirements file nor a task-runner entry.
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
        tmp_path = request.getfixturevalue("tmp_path")
        assert isinstance(tmp_path, pathlib.Path)

        return SqliteBatchTestSetup(
            data=data,
            config=self,
            base_dir=tmp_path,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class SqliteBatchTestSetup(SQLBatchTestSetup[SqliteDatasourceTestConfig]):
    def __init__(
        self,
        data: pd.DataFrame,
        config: SqliteDatasourceTestConfig,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        base_dir: pathlib.Path,
        table_name: Optional[str] = None,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> None:
        self._base_dir = base_dir
        super().__init__(
            config=config,
            data=data,
            extra_data=extra_data,
            table_name=table_name,
            engine_manager=engine_manager,
            context=context,
        )

    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        return f"sqlite:///{self.db_file_path}"

    @property
    def db_file_path(self) -> pathlib.Path:
        return self._base_dir / "database.db"

    @override
    def make_asset(self) -> TableAsset:
        return self.context.data_sources.add_sqlite(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(),
        ).add_table_asset(name=self._random_resource_name(), table_name=self.table_name)
