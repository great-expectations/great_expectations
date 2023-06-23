from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterator, List, cast
from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
import great_expectations.execution_engine.sparkdf_execution_engine
from great_expectations.compatibility import azure
from great_expectations.core.util import AzureUrl
from great_expectations.datasource.fluent import SparkAzureBlobStorageDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    AzureBlobStorageDataConnector,
)
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.spark_azure_blob_storage_datasource import (
    SparkAzureBlobStorageDatasourceError,
)
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    CSVAsset,
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
    azure_options: Dict[str, Any] | None = None
) -> SparkAzureBlobStorageDatasource:
    azure_client: azure.BlobServiceClient = cast(
        azure.BlobServiceClient, MockBlobServiceClient()
    )
    spark_abs_datasource = SparkAzureBlobStorageDatasource(
        name="spark_abs_datasource",
        azure_options=azure_options or {},
    )
    spark_abs_datasource._azure_client = azure_client
    return spark_abs_datasource


@pytest.fixture
def spark_abs_datasource() -> SparkAzureBlobStorageDatasource:
    spark_abs_datasource: SparkAzureBlobStorageDatasource = (
        _build_spark_abs_datasource()
    )
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
    "great_expectations.datasource.fluent.data_asset.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
def csv_asset(
    mock_list_keys,
    object_keys: List[str],
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
) -> _FilePathDataAsset:
    mock_list_keys.return_value = object_keys
    asset = spark_abs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
        abs_container="my_container",
    )
    return asset


@pytest.fixture
def bad_regex_config(csv_asset: CSVAsset) -> tuple[re.Pattern, str]:
    regex = re.compile(
        r"(?P<name>.+)_(?P<ssn>\d{9})_(?P<timestamp>.+)_(?P<price>\d{4})\.csv"
    )
    data_connector: AzureBlobStorageDataConnector = cast(
        AzureBlobStorageDataConnector, csv_asset._data_connector
    )
    test_connection_error_message = f"""No file belonging to account "{csv_asset.datasource._account_name}" in container "{data_connector._container}" with prefix "{data_connector._prefix}" matched regular expressions pattern "{regex.pattern}" using delimiter "{data_connector._delimiter}" for DataAsset "{csv_asset}"."""
    return regex, test_connection_error_message


@pytest.mark.integration
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


@pytest.mark.integration
def test_construct_spark_abs_datasource_with_conn_str_and_credential():
    spark_abs_datasource = SparkAzureBlobStorageDatasource(
        name="spark_abs_datasource",
        azure_options={  # Representative of format noted in official docs
            "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",
            "credential": "my_credential",
        },
    )
    # noinspection PyUnresolvedReferences
    azure_client: azure.BlobServiceClient = spark_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert spark_abs_datasource.name == "spark_abs_datasource"


@pytest.mark.integration
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


@pytest.mark.integration
def test_construct_spark_abs_datasource_with_valid_conn_str_assigns_account_name():
    spark_abs_datasource = SparkAzureBlobStorageDatasource(
        name="spark_abs_datasource",
        azure_options={  # Representative of format noted in official docs
            "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",
            "credential": "my_credential",
        },
    )
    # noinspection PyUnresolvedReferences
    azure_client: azure.BlobServiceClient = spark_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert spark_abs_datasource.name == "spark_abs_datasource"


@pytest.mark.integration
def test_construct_spark_abs_datasource_with_multiple_auth_methods_raises_error():
    # Raises error in DataContext's schema validation due to having both `account_url` and `conn_str`
    with pytest.raises(SparkAzureBlobStorageDatasourceError):
        spark_abs_datasource = SparkAzureBlobStorageDatasource(
            name="spark_abs_datasource",
            azure_options={
                "account_url": "account.blob.core.windows.net",
                "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",
                "credential": "my_credential",
            },
        )
        # noinspection PyUnresolvedReferences
        _ = spark_abs_datasource._get_azure_client()


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.azure_blob_storage_data_connector.list_azure_keys"
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
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
        abs_container="my_container",
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
    "great_expectations.datasource.fluent.data_asset.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_construct_csv_asset_directly(
    mock_azure_client, mock_list_keys, object_keys: List[str]
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
    "great_expectations.datasource.fluent.data_asset.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_csv_asset_with_batching_regex_unnamed_parameters(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = spark_abs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
        abs_container="my_container",
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
    "great_expectations.datasource.fluent.data_asset.data_connector.azure_blob_storage_data_connector.list_azure_keys"
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
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
        abs_container="my_container",
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
    "great_expectations.datasource.fluent.data_asset.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_csv_asset_with_some_batching_regex_named_parameters(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = spark_abs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(.+)_(?P<price>\d{4})\.csv",
        abs_container="my_container",
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
    "great_expectations.datasource.fluent.data_asset.data_connector.azure_blob_storage_data_connector.list_azure_keys"
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
        batching_regex=r"(.+)_(.+)_(?P<price>\d{4})\.csv",
        abs_container="my_container",
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # price is an int which will raise an error
        asset.build_batch_request(
            {"name": "alex", "timestamp": "1234567890", "price": 1300}
        )


@pytest.mark.integration
@pytest.mark.xfail(
    reason="Accessing objects on azure.storage.blob using Spark is not working, due to local credentials issues (this test is conducted using Jupyter notebook manually)."
)
def test_get_batch_list_from_fully_specified_batch_request(
    monkeypatch: pytest.MonkeyPatch,
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
):
    azure_client: azure.BlobServiceClient = cast(
        azure.BlobServiceClient, MockBlobServiceClient()
    )

    def instantiate_azure_client_spy(self) -> None:
        self._azure_client = azure_client

    monkeypatch.setattr(
        great_expectations.execution_engine.sparkdf_execution_engine.SparkDFExecutionEngine,
        "_instantiate_s3_client",
        instantiate_azure_client_spy,
        raising=True,
    )
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_abs_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
        abs_container="my_container",
        batch_metadata=asset_specified_metadata,
    )

    request = asset.build_batch_request(
        {"name": "alex", "timestamp": "20200819", "price": "1300"}
    )
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 1
    batch = batches[0]
    assert batch.batch_request.datasource_name == spark_abs_datasource.name
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
        == "spark_abs_datasource-csv_asset-name_alex-timestamp_20200819-price_1300"
    )

    request = asset.build_batch_request({"name": "alex"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 2


@pytest.mark.integration
def test_test_connection_failures(
    spark_abs_datasource: SparkAzureBlobStorageDatasource,
    bad_regex_config: tuple[re.Pattern, str],
):
    regex, test_connection_error_message = bad_regex_config
    csv_asset = CSVAsset(
        name="csv_asset",
        batching_regex=regex,
    )
    csv_asset._datasource = spark_abs_datasource
    spark_abs_datasource.assets = [
        csv_asset,
    ]
    csv_asset._data_connector = AzureBlobStorageDataConnector(
        datasource_name=spark_abs_datasource.name,
        data_asset_name=csv_asset.name,
        batching_regex=re.compile(regex),
        azure_client=spark_abs_datasource._azure_client,  # type: ignore[arg-type] # _azure_client could be None
        account_name=csv_asset.datasource._account_name,
        container="my_container",
        file_path_template_map_fn=AzureUrl.AZURE_BLOB_STORAGE_HTTPS_URL_TEMPLATE.format,
    )
    csv_asset._test_connection_error_message = test_connection_error_message

    with pytest.raises(TestConnectionError) as e:
        spark_abs_datasource.test_connection()

    assert str(e.value) == str(test_connection_error_message)


# noinspection PyUnusedLocal
@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.azure_blob_storage_data_connector.list_azure_keys"
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
        batching_regex=r".*",
        abs_container="my_container",
        batch_metadata=asset_specified_metadata,
        abs_recursive_file_discovery=True,
    )
    assert "recursive" in mock_list_keys.call_args.kwargs.keys()
    assert mock_list_keys.call_args.kwargs["recursive"] is True
