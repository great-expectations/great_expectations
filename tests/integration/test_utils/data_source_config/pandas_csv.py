import pathlib

import pandas as pd
import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.test_utils.data_source_config.base import (
    BatchSetup,
    DataSourceConfig,
)


class PandasCsvDatasourceConfig(DataSourceConfig):
    @property
    @override
    def label(self) -> str:
        return "pandas-data-frame-datasource"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.unit

    @override
    def create_batch_setup(self, data: pd.DataFrame, request: pytest.FixtureRequest) -> BatchSetup:
        tmp_path = request.getfixturevalue("tmp_path")
        assert isinstance(tmp_path, pathlib.Path)

        return PandasCsvBatchSetup(data=data, config=self, tmp_path=tmp_path)


class PandasCsvBatchSetup(BatchSetup[PandasCsvDatasourceConfig]):
    def __init__(self, config: PandasCsvDatasourceConfig, data: pd.DataFrame, tmp_path) -> None:
        super().__init__(config=config, data=data)

        self.tmp_path = tmp_path

    @override
    def make_batch(self) -> Batch:
        name = self._random_resource_name()
        return (
            self._context.data_sources.add_pandas(name)
            .add_csv_asset(name, self.csv_path)
            .add_batch_definition_whole_dataframe(name)
            .get_batch()
        )

    @override
    def setup(self) -> None:
        self.data.to_csv(self.csv_path, index=False)

    @override
    def teardown(self) -> None: ...

    @property
    def csv_path(self) -> pathlib.Path:
        return self.tmp_path / "data.csv"
