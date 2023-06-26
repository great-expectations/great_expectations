from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, Iterator, List, cast
from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
import great_expectations.execution_engine.sparkdf_execution_engine
from great_expectations.compatibility import google
from great_expectations.core.util import GCSUrl
from great_expectations.datasource.fluent import (
    SparkGoogleCloudStorageDatasource,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    GoogleCloudStorageDataConnector,
)
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    CSVAsset,
)

logger = logging.getLogger(__file__)


if not google.storage:
    pytest.skip(
        'Could not import "storage" from google.cloud in configured_asset_gcs_data_connector.py',
        allow_module_level=True,
    )


class MockGCSClient:
    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def list_blobs(
        self,
        bucket_or_name,
        max_results=None,
        prefix=None,
        delimiter=None,
        **kwargs,
    ) -> Iterator:
        return iter([])


def _build_spark_gcs_datasource(
    gcs_options: Dict[str, Any] | None = None
) -> SparkGoogleCloudStorageDatasource:
    gcs_client: google.Client = cast(google.Client, MockGCSClient())
    spark_gcs_datasource = SparkGoogleCloudStorageDatasource(
        name="spark_gcs_datasource",
        bucket_or_name="test_bucket",
        gcs_options=gcs_options or {},
    )
    spark_gcs_datasource._gcs_client = gcs_client
    return spark_gcs_datasource


@pytest.fixture
def spark_gcs_datasource() -> SparkGoogleCloudStorageDatasource:
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource = (
        _build_spark_gcs_datasource()
    )
    return spark_gcs_datasource


@pytest.fixture
def object_keys() -> List[str]:
    return [
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
    ]


@pytest.fixture
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
def csv_asset(
    mock_list_keys,
    object_keys: List[str],
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource,
) -> _FilePathDataAsset:
    mock_list_keys.return_value = object_keys
    asset = spark_gcs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
    )
    return asset


@pytest.fixture
def bad_regex_config(csv_asset: CSVAsset) -> tuple[re.Pattern, str]:
    regex = re.compile(
        r"(?P<name>.+)_(?P<ssn>\d{9})_(?P<timestamp>.+)_(?P<price>\d{4})\.csv"
    )
    data_connector: GoogleCloudStorageDataConnector = cast(
        GoogleCloudStorageDataConnector, csv_asset._data_connector
    )
    test_connection_error_message = f"""No file in bucket "{csv_asset.datasource.bucket_or_name}" with prefix "{data_connector._prefix}" matched regular expressions pattern "{regex.pattern}" using delimiter "{data_connector._delimiter}" for DataAsset "{csv_asset.name}"."""
    return regex, test_connection_error_message


@pytest.mark.integration
def test_construct_spark_gcs_datasource_without_gcs_options():
    google_cred_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not google_cred_file:
        pytest.skip('No "GOOGLE_APPLICATION_CREDENTIALS" environment variable found.')

    spark_gcs_datasource = SparkGoogleCloudStorageDatasource(
        name="spark_gcs_datasource",
        bucket_or_name="test_bucket",
        gcs_options={},
    )
    # noinspection PyUnresolvedReferences
    gcs_client: google.Client = spark_gcs_datasource._get_gcs_client()
    assert gcs_client is not None
    assert spark_gcs_datasource.name == "spark_gcs_datasource"


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.oauth2.service_account.Credentials.from_service_account_file")
@mock.patch("google.cloud.storage.Client")
def test_construct_spark_gcs_datasource_with_filename_in_gcs_options(
    mock_gcs_client, mock_gcs_service_account_credentials, mock_list_keys
):
    spark_gcs_datasource = SparkGoogleCloudStorageDatasource(
        name="spark_gcs_datasource",
        bucket_or_name="test_bucket",
        gcs_options={
            "filename": "my_filename.csv",
        },
    )
    # noinspection PyUnresolvedReferences
    gcs_client: google.Client = spark_gcs_datasource._get_gcs_client()
    assert gcs_client is not None
    assert spark_gcs_datasource.name == "spark_gcs_datasource"


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.oauth2.service_account.Credentials.from_service_account_info")
@mock.patch("google.cloud.storage.Client")
def test_construct_spark_gcs_datasource_with_info_in_gcs_options(
    mock_gcs_client, mock_gcs_service_account_credentials, mock_list_keys
):
    spark_gcs_datasource = SparkGoogleCloudStorageDatasource(
        name="spark_gcs_datasource",
        bucket_or_name="test_bucket",
        gcs_options={
            "info": "{my_csv: my_content,}",
        },
    )
    # noinspection PyUnresolvedReferences
    gcs_client: google.Client = spark_gcs_datasource._get_gcs_client()
    assert gcs_client is not None
    assert spark_gcs_datasource.name == "spark_gcs_datasource"


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_add_csv_asset_to_datasource(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_gcs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
        batch_metadata=asset_specified_metadata,
    )
    assert asset.name == "csv_asset"
    assert asset.batching_regex.match("random string") is None
    assert asset.batching_regex.match("alex_20200819_13D0.csv") is None
    m1 = asset.batching_regex.match("alex_20200819_1300.csv")
    assert m1 is not None
    assert asset.batch_metadata == asset_specified_metadata


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_construct_csv_asset_directly(
    mock_gcs_client, mock_list_keys, object_keys: List[str]
):
    mock_list_keys.return_value = object_keys
    asset = CSVAsset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",  # type: ignore[arg-type]
    )
    assert asset.name == "csv_asset"
    assert asset.batching_regex.match("random string") is None
    assert asset.batching_regex.match("alex_20200819_13D0.csv") is None
    m1 = asset.batching_regex.match("alex_20200819_1300.csv")
    assert m1 is not None


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_csv_asset_with_batching_regex_unnamed_parameters(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = spark_gcs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
    )
    options = asset.batch_request_options
    assert options == (
        "batch_request_param_1",
        "batch_request_param_2",
        "batch_request_param_3",
        "path",
    )


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_csv_asset_with_batching_regex_named_parameters(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = spark_gcs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
    )
    options = asset.batch_request_options
    assert options == (
        "name",
        "timestamp",
        "price",
        "path",
    )


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_csv_asset_with_some_batching_regex_named_parameters(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = spark_gcs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(.+)_(?P<price>\d{4})\.csv",
    )
    options = asset.batch_request_options
    assert options == (
        "name",
        "batch_request_param_2",
        "price",
        "path",
    )


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = spark_gcs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(?P<price>\d{4})\.csv",
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # price is an int which will raise an error
        asset.build_batch_request(
            {"name": "alex", "timestamp": "1234567890", "price": 1300}
        )


@pytest.mark.integration
@pytest.mark.xfail(
    reason="Accessing objects on google.cloud.storage using Spark is not working, due to local credentials issues (this test is conducted using Jupyter notebook manually)."
)
def test_get_batch_list_from_fully_specified_batch_request(
    monkeypatch: pytest.MonkeyPatch,
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource,
):
    gcs_client: google.Client = cast(google.Client, MockGCSClient())

    def instantiate_gcs_client_spy(self) -> None:
        self._gcs = gcs_client

    monkeypatch.setattr(
        great_expectations.execution_engine.sparkdf_execution_engine.SparkDFExecutionEngine,
        "_instantiate_s3_client",
        instantiate_gcs_client_spy,
        raising=True,
    )
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_gcs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
        batch_metadata=asset_specified_metadata,
    )

    request = asset.build_batch_request(
        {"name": "alex", "timestamp": "20200819", "price": "1300"}
    )
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 1
    batch = batches[0]
    assert batch.batch_request.datasource_name == spark_gcs_datasource.name
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
        == "spark_gcs_datasource-csv_asset-name_alex-timestamp_20200819-price_1300"
    )

    request = asset.build_batch_request({"name": "alex"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 2


@pytest.mark.integration
def test_test_connection_failures(
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource,
    bad_regex_config: tuple[re.Pattern, str],
):
    regex, test_connection_error_message = bad_regex_config
    csv_asset = CSVAsset(
        name="csv_asset",
        batching_regex=regex,
    )
    csv_asset._datasource = spark_gcs_datasource
    spark_gcs_datasource.assets = [
        csv_asset,
    ]
    csv_asset._data_connector = GoogleCloudStorageDataConnector(
        datasource_name=spark_gcs_datasource.name,
        data_asset_name=csv_asset.name,
        batching_regex=re.compile(regex),
        gcs_client=spark_gcs_datasource._gcs_client,
        bucket_or_name=spark_gcs_datasource.bucket_or_name,
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
    )
    csv_asset._test_connection_error_message = test_connection_error_message

    with pytest.raises(TestConnectionError) as e:
        spark_gcs_datasource.test_connection()

    assert str(e.value) == str(test_connection_error_message)


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_add_csv_asset_with_recursive_file_discovery_to_datasource(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    spark_gcs_datasource: SparkGoogleCloudStorageDatasource,
):
    """
    Tests that the gcs_recursive_file_discovery-flag is passed on
    to the list_keys-function as the recursive-parameter

    This makes the list_keys-function search and return files also
    from sub-directories on GCS, not just the files in the folder
    specified with the abs_name_starts_with-parameter
    """
    mock_list_keys.return_value = object_keys
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    spark_gcs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r".*",
        batch_metadata=asset_specified_metadata,
        gcs_recursive_file_discovery=True,
    )
    assert "recursive" in mock_list_keys.call_args.kwargs.keys()
    assert mock_list_keys.call_args.kwargs["recursive"] is True
