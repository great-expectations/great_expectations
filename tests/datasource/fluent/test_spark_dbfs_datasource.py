from __future__ import annotations

import logging
import os
import pathlib
import re
from typing import TYPE_CHECKING

import boto3
import botocore
import pytest

from great_expectations.core.partitioners import FileNamePartitionerPath
from great_expectations.datasource.fluent import SparkDBFSDatasource
from great_expectations.datasource.fluent.data_asset.path.spark.csv_asset import CSVAsset
from tests.test_utils import create_files_in_directory

if TYPE_CHECKING:
    from pyfakefs.fake_filesystem import FakeFilesystem


logger = logging.getLogger(__file__)


@pytest.fixture
def spark_dbfs_datasource(fs: FakeFilesystem, test_backends) -> SparkDBFSDatasource:
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

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

    return SparkDBFSDatasource(
        name="spark_dbfs_datasource",
        base_directory=pathlib.Path(base_directory),
    )


@pytest.mark.spark
def test_construct_spark_dbfs_datasource(spark_dbfs_datasource: SparkDBFSDatasource):
    assert spark_dbfs_datasource.name == "spark_dbfs_datasource"


@pytest.mark.spark
def test_add_csv_asset_to_datasource(spark_dbfs_datasource: SparkDBFSDatasource):
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_dbfs_datasource.add_csv_asset(
        name="csv_asset",
        batch_metadata=asset_specified_metadata,
    )
    assert asset.name == "csv_asset"
    assert asset.batch_metadata == asset_specified_metadata


@pytest.mark.unit
def test_construct_csv_asset_directly():
    asset = CSVAsset(
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.spark
@pytest.mark.xfail(
    reason="Accessing objects on pyfakefs.fake_filesystem.FakeFilesystem using Spark is not working (this test is conducted using Jupyter notebook manually)."  # noqa: E501
)
def test_get_batch_list_from_fully_specified_batch_request(
    spark_dbfs_datasource: SparkDBFSDatasource,
):
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_dbfs_datasource.add_csv_asset(
        name="csv_asset",
        batch_metadata=asset_specified_metadata,
    )

    batching_regex = re.compile(r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv")
    request = asset.build_batch_request(
        options={"name": "alex", "timestamp": "20200819", "price": "1300"},
        partitioner=FileNamePartitionerPath(regex=batching_regex),
    )
    batch = asset.get_batch(request)
    assert batch.batch_request.datasource_name == spark_dbfs_datasource.name
    assert batch.batch_request.data_asset_name == asset.name
    assert batch.batch_request.options == (
        "name",
        "timestamp",
        "price",
        "path",
    )
    assert batch.metadata == {
        "path": "alex_20200819_1300.csv",
        "name": "alex",
        "timestamp": "20200819",
        "price": "1300",
        **asset_specified_metadata,
    }
    assert batch.id == "spark_dbfs_datasource-csv_asset-name_alex-timestamp_20200819-price_1300"

    request = asset.build_batch_request(
        options={"name": "alex"}, partitioner=FileNamePartitionerPath(regex=batching_regex)
    )
    assert len(asset.get_batch_identifiers_list(request)) == 2
