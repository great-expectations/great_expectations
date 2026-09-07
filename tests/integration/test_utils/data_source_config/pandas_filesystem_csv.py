import pathlib
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

import pandas as pd
import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import CSVAsset
from great_expectations.datasource.fluent.interfaces import Batch
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
@dataclass(frozen=True)
class PandasFilesystemCsvDatasourceTestConfig(DataSourceTestConfig):
    DATA_SOURCE_SPEC = DataSourceSpec(
        label="pandas-filesystem-csv",
        # The same public name the in-memory pandas record carries: both describe pandas to a
        # user, and the public name names the data source rather than the harness variant that
        # exercises it.
        public_name="Pandas",
        provisioning=DataSourceProvisioning.LOCAL_FILE,
        execution_engine=ExecutionEngineKind.PANDAS,
        fluent_types=frozenset({"pandas_filesystem"}),
        marker="filesystem",
        # Shared, not dedicated: `filesystem` names tests that use the filesystem as their storage
        # backend, which is a class of tests rather than this data source's tests alone. More than
        # one record may legitimately declare it, so it must not be checked for collision as though
        # it named this data source by itself.
        marker_scope=MarkerScope.SHARED,
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="filesystem"),
        # The shared canonical expectation parameterization runs against this config in the
        # `filesystem` lane today, through every one of its expectation modules. Declaring the
        # tier states that existing result; it switches nothing on. The marker and CI lane the
        # claim obliges are already declared above.
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        # No dev_requirements_file and no task_runner_marker: the task runner's dependency map has
        # no key for `filesystem`, because running these tests installs nothing beyond the base
        # development requirements and starts no service.
    )

    # see options: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    read_options: dict[str, Any] = field(default_factory=dict)
    # see options: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
    write_options: dict[str, Any] = field(default_factory=dict)

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
        tmp_path = request.getfixturevalue("tmp_path")
        assert isinstance(tmp_path, pathlib.Path)

        return PandasFilesystemCsvBatchTestSetup(
            data=data,
            config=self,
            base_dir=tmp_path,
            context=context,
        )


class PandasFilesystemCsvBatchTestSetup(
    BatchTestSetup[PandasFilesystemCsvDatasourceTestConfig, CSVAsset]
):
    def __init__(
        self,
        config: PandasFilesystemCsvDatasourceTestConfig,
        data: pd.DataFrame,
        base_dir: pathlib.Path,
        context: AbstractDataContext,
    ) -> None:
        super().__init__(config=config, data=data, context=context)
        self._base_dir = base_dir

    @override
    def make_asset(self) -> CSVAsset:
        return self.context.data_sources.add_pandas_filesystem(
            name=self._random_resource_name(), base_directory=self._base_dir
        ).add_csv_asset(
            name=self._random_resource_name(),
            **self.config.read_options,
        )

    @override
    def make_batch(self) -> Batch:
        return (
            self.make_asset()
            .add_batch_definition_path(name=self._random_resource_name(), path=self.csv_path)
            .get_batch()
        )

    @override
    def setup(self) -> None:
        file_path = self._base_dir / self.csv_path
        self.data.to_csv(file_path, index=False, **self.config.write_options)

    @override
    def teardown(self) -> None: ...

    @property
    def csv_path(self) -> pathlib.Path:
        return pathlib.Path("data.csv")
