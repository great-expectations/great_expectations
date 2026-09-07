from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Mapping, Optional

from great_expectations.compatibility.pydantic import BaseSettings
from great_expectations.compatibility.typing_extensions import override
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


@register_sql_config
class BigQueryDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="big-query",
        public_name="BigQuery",
        marker="bigquery",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"bigquery"}),
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="bigquery"),
        # BigQuery calls its schemas "datasets", so a per-test schema means a per-test
        # dataset. Datasets are project-level objects, which makes them a poor unit of
        # test isolation here: creating and dropping one per test config adds two DDL
        # round trips to every setup and teardown, and any run killed before teardown
        # leaves an orphan that only an out-of-band sweep can find.
        #
        # None of that buys isolation we don't already have. Table names carry a uuid4
        # suffix, so concurrent runs cannot collide regardless of which dataset they
        # share. Tests therefore create their tables directly in the configured CI
        # dataset, and cleanup only ever has to reason about tables in one known place.
        #
        # A test that genuinely needs schema-qualified coverage on BigQuery should say
        # so explicitly rather than have every test pay for it; passing `schema_name`
        # while this is False raises, so that need surfaces as a clear error rather
        # than silently doing the wrong thing.
        uses_schema=False,
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        dev_requirements_file="reqs/requirements-dev-bigquery.txt",
        task_runner_marker="bigquery",
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
        return BigQueryBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class BigQueryBatchTestSetup(SQLBatchTestSetup[BigQueryDatasourceTestConfig]):
    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        return self.big_query_connection_config.build_connection_string(dataset=schema)

    @override
    def make_asset(self) -> TableAsset:
        return self.context.data_sources.add_bigquery(
            name=self._random_resource_name(),
            connection_string=self.build_connection_string(schema=self.schema),
        ).add_table_asset(
            name=self._random_resource_name(),
            table_name=self.table_name,
        )

    @cached_property
    def big_query_connection_config(self) -> BigQueryConnectionConfig:
        return BigQueryConnectionConfig()  # type: ignore[call-arg]  # retrieves env vars


class BigQueryConnectionConfig(BaseSettings):
    """Environment variables for BigQuery connection.
    These are injected in via CI, but when running locally, you may use your own credentials.
    GOOGLE_APPLICATION_CREDENTIALS must be kept secret
    """

    GE_TEST_GCP_PROJECT: str
    GE_TEST_BIGQUERY_DATASET: str
    GOOGLE_APPLICATION_CREDENTIALS: str

    def build_connection_string(self, dataset: str | None = None) -> str:
        dataset = dataset or self.GE_TEST_BIGQUERY_DATASET
        return f"bigquery://{self.GE_TEST_GCP_PROJECT}/{dataset}?credentials_path={self.GOOGLE_APPLICATION_CREDENTIALS}"
