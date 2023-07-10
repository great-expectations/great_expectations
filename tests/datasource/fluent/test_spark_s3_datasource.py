from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING, List, cast

import pandas as pd
import pytest
from moto import mock_s3

import great_expectations.exceptions as ge_exceptions
from great_expectations.compatibility import aws
from great_expectations.core.util import S3Url
from great_expectations.datasource.fluent import SparkS3Datasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    S3DataConnector,
)
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.spark_file_path_datasource import CSVAsset

if TYPE_CHECKING:
    from botocore.client import BaseClient


logger = logging.getLogger(__file__)


@pytest.fixture()
def aws_region_name() -> str:
    return "us-east-1"


@pytest.fixture()
def aws_s3_bucket_name() -> str:
    return "test_bucket"


@pytest.fixture(scope="function")
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.mark.skipif(not aws.boto3)
@pytest.fixture
def s3_mock(aws_credentials, aws_region_name: str) -> BaseClient:
    with mock_s3():
        client = aws.boto3.client("s3", region_name=aws_region_name)
        yield client


@pytest.fixture
def s3_bucket(s3_mock: BaseClient, aws_s3_bucket_name: str) -> str:
    bucket_name: str = aws_s3_bucket_name
    s3_mock.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture
def spark_s3_datasource(s3_mock, s3_bucket: str) -> SparkS3Datasource:
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
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
def csv_asset(spark_s3_datasource: SparkS3Datasource) -> _FilePathDataAsset:
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
        header=True,
        infer_schema=True,
    )
    return asset


@pytest.fixture
def bad_regex_config(csv_asset: CSVAsset) -> tuple[re.Pattern, str]:
    regex = re.compile(
        r"(?P<name>.+)_(?P<ssn>\d{9})_(?P<timestamp>.+)_(?P<price>\d{4})\.csv"
    )
    data_connector: S3DataConnector = cast(S3DataConnector, csv_asset._data_connector)
    test_connection_error_message = f"""No file in bucket "{csv_asset.datasource.bucket}" with prefix "{data_connector._prefix}" matched regular expressions pattern "{regex.pattern}" using delimiter "{data_connector._delimiter}" for DataAsset "{csv_asset.name}"."""
    return regex, test_connection_error_message


@pytest.mark.integration
def test_construct_spark_s3_datasource(spark_s3_datasource: SparkS3Datasource):
    assert spark_s3_datasource.name == "spark_s3_datasource"


@pytest.mark.integration
def test_add_csv_asset_to_datasource(spark_s3_datasource: SparkS3Datasource):
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
        header=True,
        infer_schema=True,
        batch_metadata=asset_specified_metadata,
    )
    assert asset.name == "csv_asset"
    assert asset.batching_regex.match("random string") is None
    assert asset.batching_regex.match("alex_20200819_13D0.csv") is None
    m1 = asset.batching_regex.match("alex_20200819_1300.csv")
    assert m1 is not None
    assert asset.batch_metadata == asset_specified_metadata


@pytest.mark.integration
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


@pytest.mark.integration
def test_csv_asset_with_batching_regex_unnamed_parameters(
    spark_s3_datasource: SparkS3Datasource,
):
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
        header=True,
        infer_schema=True,
    )
    options = asset.batch_request_options
    assert options == (
        "batch_request_param_1",
        "batch_request_param_2",
        "batch_request_param_3",
        "path",
    )


@pytest.mark.integration
def test_csv_asset_with_batching_regex_named_parameters(
    spark_s3_datasource: SparkS3Datasource,
):
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
        header=True,
        infer_schema=True,
    )
    options = asset.batch_request_options
    assert options == (
        "name",
        "timestamp",
        "price",
        "path",
    )


@pytest.mark.integration
def test_csv_asset_with_some_batching_regex_named_parameters(
    spark_s3_datasource: SparkS3Datasource,
):
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(.+)_(?P<price>\d{4})\.csv",
        header=True,
        infer_schema=True,
    )
    options = asset.batch_request_options
    assert options == (
        "name",
        "batch_request_param_2",
        "price",
        "path",
    )


@pytest.mark.integration
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    spark_s3_datasource: SparkS3Datasource,
):
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(?P<price>\d{4})\.csv",
        header=True,
        infer_schema=True,
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # price is an int which will raise an error
        asset.build_batch_request(
            {"name": "alex", "timestamp": "1234567890", "price": 1300}
        )


@pytest.mark.integration
@pytest.mark.xfail(
    reason="Accessing objects on moto.mock_s3 using Spark is not working (this test is conducted using Jupyter notebook manually)."
)
def test_get_batch_list_from_fully_specified_batch_request(
    spark_s3_datasource: SparkS3Datasource,
):
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
        header=True,
        infer_schema=True,
        batch_metadata=asset_specified_metadata,
    )

    request = asset.build_batch_request(
        {"name": "alex", "timestamp": "20200819", "price": "1300"}
    )
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 1
    batch = batches[0]
    assert batch.batch_request.datasource_name == spark_s3_datasource.name
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
        **asset_specified_metadata,
    }
    assert (
        batch.id
        == "spark_s3_datasource-csv_asset-name_alex-timestamp_20200819-price_1300"
    )

    request = asset.build_batch_request({"name": "alex"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 2


@pytest.mark.integration
def test_test_connection_failures(
    s3_mock,
    spark_s3_datasource: SparkS3Datasource,
    bad_regex_config: tuple[re.Pattern, str],
):
    regex, test_connection_error_message = bad_regex_config
    csv_asset = CSVAsset(
        name="csv_asset",
        batching_regex=regex,
    )
    csv_asset._datasource = spark_s3_datasource
    spark_s3_datasource.assets = [
        csv_asset,
    ]
    csv_asset._data_connector = S3DataConnector(
        datasource_name=spark_s3_datasource.name,
        data_asset_name=csv_asset.name,
        batching_regex=re.compile(regex),
        s3_client=s3_mock,
        bucket=spark_s3_datasource.bucket,
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    csv_asset._test_connection_error_message = test_connection_error_message

    with pytest.raises(TestConnectionError) as e:
        spark_s3_datasource.test_connection()

    assert str(e.value) == str(test_connection_error_message)


@pytest.mark.integration
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
        batching_regex=r".*",
        header=True,
        infer_schema=True,
        batch_metadata=asset_specified_metadata,
        s3_recursive_file_discovery=True,
    )
    recursion_match = asset.batching_regex.match(".*/.*.csv")
    assert recursion_match is not None
