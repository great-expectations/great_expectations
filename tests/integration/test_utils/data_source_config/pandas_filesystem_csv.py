import pathlib

import pandas as pd
import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.test_utils.data_source_config.base import (
    BatchTestSetup,
    DataSourceTestConfig,
)


class PandasFilesystemCsvDatasourceTestConfig(DataSourceTestConfig):
    @property
    @override
    def label(self) -> str:
        return "pandas-filesystem-csv"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.filesystem

    @override
    def create_batch_setup(
        self, data: pd.DataFrame, request: pytest.FixtureRequest
    ) -> BatchTestSetup:
        tmp_path = request.getfixturevalue("tmp_path")
        assert isinstance(tmp_path, pathlib.Path)

        return PandasFilesystemCsvBatchTestSetup(
            data=data,
            config=self,
            base_dir=tmp_path,
        )


class PandasFilesystemCsvBatchTestSetup(BatchTestSetup[PandasFilesystemCsvDatasourceTestConfig]):
    def __init__(
        self,
        config: PandasFilesystemCsvDatasourceTestConfig,
        data: pd.DataFrame,
        base_dir: pathlib.Path,
    ) -> None:
        super().__init__(config=config, data=data)
        self._base_dir = base_dir

    @override
    def make_batch(self) -> Batch:
        name = self._random_resource_name()
        path = self._base_dir

        return (
            self._context.data_sources.add_pandas_filesystem(name=name, base_directory=path)
            .add_csv_asset(name=name)
            .add_batch_definition_path(name=name, path=self.csv_path)
            .get_batch()
        )

    @override
    def setup(self) -> None:
        file_path = self._base_dir / self.csv_path
        self.data.to_csv(file_path, index=False)

    @override
    def teardown(self) -> None: ...

    @property
    def csv_path(self) -> pathlib.Path:
        return pathlib.Path("data.csv")
