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
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)
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
            "alex_20200809_1000.csv",
            "eugene_20200809_1500.csv",
            "james_20200811_1009.csv",
            "abe_20200809_1040.csv",
            "will_20200809_1002.csv",
            "james_20200713_1567.csv",
            "eugene_20201129_1900.csv",
            "will_20200810_1001.csv",
            "james_20200810_1003.csv",
            "alex_20200819_1300.csv",
        ],
    )

    pandas_dbfs_datasource = PandasDBFSDatasource(
        name="pandas_dbfs_datasource",
        base_directory=pathlib.Path(base_directory),
    )
    pandas_dbfs_datasource._data_context = empty_data_context

    return pandas_dbfs_datasource


@pytest.fixture
def csv_asset(pandas_dbfs_datasource: PandasDBFSDatasource) -> PathDataAsset:
    asset = pandas_dbfs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
    )
    return asset


@pytest.mark.filesystem
def test_construct_pandas_dbfs_datasource(pandas_dbfs_datasource: PandasDBFSDatasource):
    assert pandas_dbfs_datasource.name == "pandas_dbfs_datasource"


@pytest.mark.filesystem
def test_add_csv_asset_to_datasource(pandas_dbfs_datasource: PandasDBFSDatasource):
    asset = pandas_dbfs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
    )
    assert asset.name == "csv_asset"
    assert asset.batching_regex.match("random string") is None
    assert asset.batching_regex.match("alex_20200819_13D0.csv") is None
    m1 = asset.batching_regex.match("alex_20200819_1300.csv")
    assert m1 is not None


@pytest.mark.filesystem
def test_construct_csv_asset_directly():
    # noinspection PyTypeChecker
    asset = CSVAsset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
    )
    assert asset.name == "csv_asset"
    assert asset.batching_regex.match("random string") is None
    assert asset.batching_regex.match("alex_20200819_13D0.csv") is None
    m1 = asset.batching_regex.match("alex_20200819_1300.csv")
    assert m1 is not None


@pytest.mark.filesystem
def test_get_batch_list_from_fully_specified_batch_request(
    pandas_dbfs_datasource: PandasDBFSDatasource,
):
    asset = pandas_dbfs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
    )

    request = asset.build_batch_request({"name": "alex", "timestamp": "20200819", "price": "1300"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 1
    batch = batches[0]
    assert batch.batch_request.datasource_name == pandas_dbfs_datasource.name
    assert batch.batch_request.data_asset_name == asset.name
    assert batch.batch_request.options == {
        "path": "alex_20200819_1300.csv",
        "name": "alex",
        "timestamp": "20200819",
        "price": "1300",
    }
    assert batch.metadata == {
        "path": "alex_20200819_1300.csv",
        "name": "alex",
        "timestamp": "20200819",
        "price": "1300",
    }
    assert batch.id == "pandas_dbfs_datasource-csv_asset-name_alex-timestamp_20200819-price_1300"

    request = asset.build_batch_request({"name": "alex"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 2
