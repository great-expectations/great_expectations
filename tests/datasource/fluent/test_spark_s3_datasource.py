from __future__ import annotations

from typing import TYPE_CHECKING, List

import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.datasource.fluent import SparkS3Datasource
from great_expectations.datasource.fluent.data_asset.path.spark.csv_asset import CSVAsset
from great_expectations.datasource.fluent.interfaces import TestConnectionError

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
        PathDataAsset,
    )


@pytest.fixture()
def aws_s3_bucket_name() -> str:
    return "test_bucket"


@pytest.fixture
def s3_bucket(s3_mock: BaseClient, aws_s3_bucket_name: str) -> str:
    bucket_name: str = aws_s3_bucket_name
    s3_mock.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture
def spark_s3_datasource(s3_mock, s3_bucket: str) -> SparkS3Datasource:
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
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
        "subfolder/for_recursive_search.csv",
    ]

    for key in keys:
        s3_mock.put_object(
            Bucket=s3_bucket,
            Body=test_df.to_csv(index=False).encode("utf-8"),
            Key=key,
        )

    return SparkS3Datasource(
        name="spark_s3_datasource",
        bucket=s3_bucket,
    )


@pytest.fixture
def csv_asset(spark_s3_datasource: SparkS3Datasource) -> PathDataAsset:
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    return asset


@pytest.mark.unit
def test_construct_spark_s3_datasource(spark_s3_datasource: SparkS3Datasource):
    assert spark_s3_datasource.name == "spark_s3_datasource"


@pytest.mark.unit
def test_add_csv_asset_to_datasource(spark_s3_datasource: SparkS3Datasource):
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
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


@pytest.mark.unit
def test_csv_asset_with_batching_regex_named_parameters(
    spark_s3_datasource: SparkS3Datasource,
):
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_def = asset.add_batch_definition_monthly(name="batch def", regex=batching_regex)
    options = asset.get_batch_parameters_keys(partitioner=batch_def.partitioner)
    assert options == ("path", "year", "month")


@pytest.mark.unit
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    spark_s3_datasource: SparkS3Datasource,
):
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # price is an int which will raise an error
        asset.build_batch_request({"year": "2024", "month": 5})


@pytest.mark.unit
def test_add_csv_asset_with_recursive_file_discovery_to_datasource(
    spark_s3_datasource: SparkS3Datasource,
):
    """
    Tests that files from the subfolder(s) is returned
    when the s3_recursive_file_discovery-flag is set to True

    This makes the list_keys-function search and return files also
    from sub-directories on S3, not just the files in the folder
    specified with the s3_name_starts_with-parameter
    """
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset_recursive",
        header=True,
        infer_schema=True,
        batch_metadata=asset_specified_metadata,
        s3_recursive_file_discovery=True,
    )
    assert asset.batch_metadata == asset_specified_metadata
    assert "subfolder/for_recursive_search.csv" in asset._data_connector.get_data_references()


_PARTITIONED_DAY_PREFIX = "my_prefix/yyyy=2025/mm=03/dd=09/"


@pytest.fixture
def partitioned_day_keys(s3_mock: BaseClient, s3_bucket: str) -> List[str]:
    """Objects that all sit one level below ``_PARTITIONED_DAY_PREFIX``."""
    keys = [f"{_PARTITIONED_DAY_PREFIX}hh={hh}/part-0.parquet" for hh in ("00", "01")]
    for key in keys:
        s3_mock.put_object(Bucket=s3_bucket, Key=key, Body=b"PAR1")
    return keys


@pytest.mark.unit
def test_recursive_file_lookup_drives_s3_file_discovery(
    s3_bucket: str, partitioned_day_keys: List[str]
):
    """
    A Spark S3 asset exposes ``recursive_file_lookup`` (Spark's own
    ``recursiveFileLookup`` reader option). Setting it must make the asset
    discover objects one level below ``s3_prefix``, not only when the separate
    ``s3_recursive_file_discovery`` connect option is passed.
    """
    datasource = SparkS3Datasource(name="spark_s3_datasource", bucket=s3_bucket)

    asset = datasource.add_parquet_asset(  # type: ignore[attr-defined]
        name="partitioned_day",
        s3_prefix=_PARTITIONED_DAY_PREFIX,
        recursive_file_lookup=True,
    )

    assert asset.recursive_file_lookup is True
    assert sorted(asset._data_connector.get_data_references()) == partitioned_day_keys


@pytest.mark.unit
def test_recursive_file_lookup_drives_s3_file_discovery_for_directory_asset(
    s3_bucket: str, partitioned_day_keys: List[str]
):
    datasource = SparkS3Datasource(name="spark_s3_datasource", bucket=s3_bucket)

    asset = datasource.add_directory_parquet_asset(  # type: ignore[attr-defined]
        name="partitioned_day",
        data_directory=_PARTITIONED_DAY_PREFIX,
        s3_prefix=_PARTITIONED_DAY_PREFIX,
        recursive_file_lookup=True,
    )

    assert asset.recursive_file_lookup is True
    assert sorted(asset._data_connector.get_data_references()) == partitioned_day_keys


@pytest.mark.unit
def test_explicit_s3_recursive_file_discovery_overrides_recursive_file_lookup(
    s3_bucket: str, partitioned_day_keys: List[str]
):
    datasource = SparkS3Datasource(name="spark_s3_datasource", bucket=s3_bucket)

    with pytest.raises(TestConnectionError, match='recursive file discovery set to "False"'):
        datasource.add_parquet_asset(  # type: ignore[attr-defined]
            name="partitioned_day",
            s3_prefix=_PARTITIONED_DAY_PREFIX,
            recursive_file_lookup=True,
            s3_recursive_file_discovery=False,
        )


@pytest.mark.unit
def test_recursive_file_lookup_reflected_in_test_connection_error_message(
    s3_mock: BaseClient, s3_bucket: str
):
    # zero-size objects are skipped during discovery, so nothing is found below this prefix
    s3_mock.put_object(Bucket=s3_bucket, Key="empty_prefix/sub/placeholder", Body=b"")
    datasource = SparkS3Datasource(name="spark_s3_datasource", bucket=s3_bucket)

    with pytest.raises(TestConnectionError, match='recursive file discovery set to "True"'):
        datasource.add_parquet_asset(  # type: ignore[attr-defined]
            name="empty_prefix",
            s3_prefix="empty_prefix/",
            recursive_file_lookup=True,
        )
