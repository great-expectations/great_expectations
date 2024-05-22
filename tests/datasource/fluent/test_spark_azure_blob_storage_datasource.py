from __future__ import annotations

import logging
from typing import Any, Dict, Iterator, List, cast
from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.compatibility import azure
from great_expectations.datasource.fluent import SparkAzureBlobStorageDatasource
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.csv_asset import CSVAsset
from great_expectations.datasource.fluent.spark_azure_blob_storage_datasource import (
    SparkAzureBlobStorageDatasourceError,
)

logger = logging.getLogger(__file__)


if not (azure.storage and azure.BlobServiceClient and azure.ContainerClient):  # type: ignore[truthy-function] # False if NotImported
    pytest.skip(
        'Could not import "azure.storage.blob" from Microsoft Azure cloud',
        allow_module_level=True,
    )


class MockContainerClient:
    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def walk_blobs(
        self,
        name_starts_with: str | None = None,
        include: Any | None = None,
        delimiter: str = "/",
        **kwargs,
    ) -> Iterator:
        return iter([])


class MockBlobServiceClient:
    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_container_client(self, container: str) -> azure.ContainerClient:
        return cast(azure.ContainerClient, MockContainerClient())


def _build_spark_abs_datasource(
    azure_options: Dict[str, Any] | None = None,
) -> SparkAzureBlobStorageDatasource:
    azure_client: azure.BlobServiceClient = cast(azure.BlobServiceClient, MockBlobServiceClient())
    spark_abs_datasource = SparkAzureBlobStorageDatasource(
        name="spark_abs_datasource",
        azure_options=azure_options or {},
    )
    spark_abs_datasource._azure_client = azure_client
    return spark_abs_datasource


@pytest.fixture
def spark_abs_datasource() -> SparkAzureBlobStorageDatasource:
    spark_abs_datasource: SparkAzureBlobStorageDatasource = _build_spark_abs_datasource()
    return spark_abs_datasource


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
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
def csv_asset(
    mock_list_keys,
    object_keys: List[str],
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
) -> PathDataAsset:
    mock_list_keys.return_value = object_keys
    asset = spark_abs_datasource.add_csv_asset(
        name="csv_asset",
        abs_container="my_container",
    )
    return asset


@pytest.mark.unit
def test_construct_spark_abs_datasource_with_account_url_and_credential():
    spark_abs_datasource = SparkAzureBlobStorageDatasource(
        name="spark_abs_datasource",
        azure_options={
            "account_url": "my_account_url.blob.core.windows.net",
            "credential": "my_credential",
        },
    )
    # noinspection PyUnresolvedReferences
    azure_client: azure.BlobServiceClient = spark_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert spark_abs_datasource.name == "spark_abs_datasource"


@pytest.mark.unit
def test_construct_spark_abs_datasource_with_conn_str_and_credential():
    spark_abs_datasource = SparkAzureBlobStorageDatasource(
        name="spark_abs_datasource",
        azure_options={  # Representative of format noted in official docs
            "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",  # noqa: E501
            "credential": "my_credential",
        },
    )
    # noinspection PyUnresolvedReferences
    azure_client: azure.BlobServiceClient = spark_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert spark_abs_datasource.name == "spark_abs_datasource"


@pytest.mark.unit
def test_construct_spark_abs_datasource_with_valid_account_url_assigns_account_name():
    spark_abs_datasource = SparkAzureBlobStorageDatasource(
        name="spark_abs_datasource",
        azure_options={
            "account_url": "my_account_url.blob.core.windows.net",
            "credential": "my_credential",
        },
    )
    # noinspection PyUnresolvedReferences
    azure_client: azure.BlobServiceClient = spark_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert spark_abs_datasource.name == "spark_abs_datasource"


@pytest.mark.unit
def test_construct_spark_abs_datasource_with_valid_conn_str_assigns_account_name():
    spark_abs_datasource = SparkAzureBlobStorageDatasource(
        name="spark_abs_datasource",
        azure_options={  # Representative of format noted in official docs
            "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",  # noqa: E501
            "credential": "my_credential",
        },
    )
    # noinspection PyUnresolvedReferences
    azure_client: azure.BlobServiceClient = spark_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert spark_abs_datasource.name == "spark_abs_datasource"


@pytest.mark.unit
def test_construct_spark_abs_datasource_with_multiple_auth_methods_raises_error():
    # Raises error in DataContext's schema validation due to having both `account_url` and `conn_str`  # noqa: E501
    with pytest.raises(SparkAzureBlobStorageDatasourceError):
        spark_abs_datasource = SparkAzureBlobStorageDatasource(
            name="spark_abs_datasource",
            azure_options={
                "account_url": "account.blob.core.windows.net",
                "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",  # noqa: E501
                "credential": "my_credential",
            },
        )
        # noinspection PyUnresolvedReferences
        _ = spark_abs_datasource._get_azure_client()


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_add_csv_asset_to_datasource(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_abs_datasource.add_csv_asset(
        name="csv_asset",
        abs_container="my_container",
        batch_metadata=asset_specified_metadata,
    )
    assert asset.name == "csv_asset"
    assert asset.batch_metadata == asset_specified_metadata


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_construct_csv_asset_directly(mock_azure_client, mock_list_keys, object_keys: List[str]):
    mock_list_keys.return_value = object_keys
    asset = CSVAsset(  # type: ignore[call-arg] # missing args
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_csv_asset_with_batching_regex_named_parameters(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = spark_abs_datasource.add_csv_asset(
        name="csv_asset",
        abs_container="my_container",
    )
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_def = asset.add_batch_definition_monthly(name="batch def", regex=batching_regex)
    options = asset.get_batch_parameters_keys(partitioner=batch_def.partitioner)
    assert options == ("path", "year", "month")


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = spark_abs_datasource.add_csv_asset(
        name="csv_asset",
        abs_container="my_container",
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # price is an int which will raise an error
        asset.build_batch_request({"name": "alex", "timestamp": "1234567890", "price": 1300})


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_add_csv_asset_with_recursive_file_discovery_to_datasource(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
):
    """
    Tests that the abs_recursive_file_discovery-flag is passed on
    to the list_keys-function as the recursive-parameter

    This makes the list_keys-function search and return files also
    from sub-directories on Azure, not just the files in the folder
    specified with the abs_name_starts_with-parameter
    """
    mock_list_keys.return_value = object_keys
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    spark_abs_datasource.add_csv_asset(
        name="csv_asset",
        abs_container="my_container",
        batch_metadata=asset_specified_metadata,
        abs_recursive_file_discovery=True,
    )
    assert "recursive" in mock_list_keys.call_args.kwargs
    assert mock_list_keys.call_args.kwargs["recursive"] is True
