from __future__ import annotations

import logging
import os
import pathlib
from typing import TYPE_CHECKING

import boto3
import botocore
import pytest

from great_expectations.datasource.fluent import PandasDBFSDatasource
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import CSVAsset
from great_expectations.datasource.fluent.dynamic_pandas import PANDAS_VERSION
from tests.test_utils import create_files_in_directory

if TYPE_CHECKING:
    from pyfakefs.fake_filesystem import FakeFilesystem

    from great_expectations.data_context import FileDataContext


logger = logging.getLogger(__file__)


# apply markers to entire test module
pytestmark = [
    pytest.mark.skipif(
        PANDAS_VERSION < 1.2, reason=f"Fluent pandas not supported on {PANDAS_VERSION}"
    )
]


@pytest.fixture
def pandas_dbfs_datasource(
    empty_data_context: FileDataContext, fs: FakeFilesystem
) -> PandasDBFSDatasource:
    # Copy boto modules into fake filesystem (see https://github.com/spulec/moto/issues/1682#issuecomment-645016188)
    for module in [boto3, botocore]:
        module_dir = pathlib.Path(module.__file__).parent
        fs.add_real_directory(module_dir, lazy_read=False)

    # Copy google credentials into fake filesystem if they exist on your filesystem
    google_cred_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if google_cred_file:
        fs.add_real_file(google_cred_file)

    base_directory: str = "/dbfs/great_expectations"
    fs.create_dir(base_directory)

    fs.create_dir(empty_data_context.root_directory)

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "yellow_tripdata_sample_2024-01.csv",
            "yellow_tripdata_sample_2024-02.csv",
            "yellow_tripdata_sample_2024-03.csv",
            "yellow_tripdata_sample_2024-04.csv",
            "yellow_tripdata_sample_2024-05.csv",
            "yellow_tripdata_sample_2024-06.csv",
            "yellow_tripdata_sample_2024-07.csv",
            "yellow_tripdata_sample_2024-08.csv",
            "yellow_tripdata_sample_2024-09.csv",
            "yellow_tripdata_sample_2024-10.csv",
            "yellow_tripdata_sample_2024-11.csv",
            "yellow_tripdata_sample_2024-12.csv",
        ],
    )

    pandas_dbfs_datasource = PandasDBFSDatasource(
        name="pandas_dbfs_datasource",
        base_directory=pathlib.Path(base_directory),
    )
    pandas_dbfs_datasource._data_context = empty_data_context

    return pandas_dbfs_datasource


@pytest.mark.filesystem
def test_construct_pandas_dbfs_datasource(pandas_dbfs_datasource: PandasDBFSDatasource):
    assert pandas_dbfs_datasource.name == "pandas_dbfs_datasource"


@pytest.mark.filesystem
def test_add_csv_asset_to_datasource(pandas_dbfs_datasource: PandasDBFSDatasource):
    asset = pandas_dbfs_datasource.add_csv_asset(
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.filesystem
def test_construct_csv_asset_directly():
    # noinspection PyTypeChecker
    asset = CSVAsset(
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.filesystem
def test_get_batch_list_from_fully_specified_batch_request(
    pandas_dbfs_datasource: PandasDBFSDatasource,
):
    asset = pandas_dbfs_datasource.add_csv_asset(
        name="csv_asset",
    )
    regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_def = asset.add_batch_definition_monthly(name="batch def", regex=regex)
    batch_parameters = {"year": "2024", "month": "05"}
    batch = batch_def.get_batch(batch_parameters=batch_parameters)

    assert batch.batch_request.datasource_name == pandas_dbfs_datasource.name
    assert batch.batch_request.data_asset_name == asset.name
    path = "yellow_tripdata_sample_2024-05.csv"
    assert batch.batch_request.options == {"path": path, "year": "2024", "month": "05"}
    assert batch.metadata == {"path": path, "year": "2024", "month": "05"}
    assert batch.id == "pandas_dbfs_datasource-csv_asset-year_2024-month_05"
