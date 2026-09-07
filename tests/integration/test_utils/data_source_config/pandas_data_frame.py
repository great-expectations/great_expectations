from typing import Mapping, Optional

import pandas as pd
import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset
from tests.integration.sql_session_manager import SessionSQLEngineManager
from tests.integration.test_utils.data_source_config.base import (
    BatchTestSetup,
    DataSourceTestConfig,
)
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    DataSourceSpec,
    ExecutionEngineKind,
    MarkerScope,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import register_data_source_config


@register_data_source_config
class PandasDataFrameDatasourceTestConfig(DataSourceTestConfig):
    DATA_SOURCE_SPEC = DataSourceSpec(
        label="pandas-data-frame",
        public_name="Pandas",
        provisioning=DataSourceProvisioning.IN_PROCESS,
        execution_engine=ExecutionEngineKind.PANDAS,
        fluent_types=frozenset({"pandas"}),
        marker="unit",
        # Shared, not dedicated: `unit` selects every unit test in this repository, not this data
        # source's tests. It names a class of tests that this data source happens to belong to, so
        # it can legitimately be declared by more than one record and must not be checked for
        # collision as though it named this data source alone.
        marker_scope=MarkerScope.SHARED,
        ci_lane=CiLaneRef(workflow_job="unit-tests", marker_token="unit"),
        # The shared canonical expectation parameterization runs against this config in the
        # `unit` lane today, through every one of its expectation modules. Declaring the tier
        # states that existing result; it switches nothing on. The marker and CI lane the claim
        # obliges are already declared above.
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        # No dev_requirements_file and no task_runner_marker: the task runner's dependency map has
        # no key for `unit`, because running these tests installs nothing beyond the base
        # development requirements and starts no service.
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
        assert not extra_data, "extra_data is not supported for this data source."
        return PandasDataFrameBatchTestSetup(data=data, config=self, context=context)


class PandasDataFrameBatchTestSetup(
    BatchTestSetup[PandasDataFrameDatasourceTestConfig, DataFrameAsset]
):
    @override
    def make_asset(self) -> DataFrameAsset:
        return self.context.data_sources.add_pandas(
            self._random_resource_name()
        ).add_dataframe_asset(self._random_resource_name())

    @override
    def make_batch(self) -> Batch:
        return (
            self.make_asset()
            .add_batch_definition_whole_dataframe(self._random_resource_name())
            .get_batch(batch_parameters={"dataframe": self.data})
        )

    @override
    def setup(self) -> None: ...

    @override
    def teardown(self) -> None: ...
