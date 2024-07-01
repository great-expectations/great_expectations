from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, cast
from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.compatibility import azure
from great_expectations.datasource.fluent import PandasAzureBlobStorageDatasource
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import CSVAsset
from great_expectations.datasource.fluent.dynamic_pandas import PANDAS_VERSION
from great_expectations.datasource.fluent.pandas_azure_blob_storage_datasource import (
    PandasAzureBlobStorageDatasourceError,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.file_data_context import (
        FileDataContext,
    )

logger = logging.getLogger(__file__)


if not (azure.storage and azure.BlobServiceClient and azure.ContainerClient):  # type: ignore[truthy-function] # False if NotImported
    pytest.skip(
        'Could not import "azure.storage.blob" from Microsoft Azure cloud',
        allow_module_level=True,
    )


# apply markers to entire test module
pytestmark = [
    pytest.mark.skipif(
        PANDAS_VERSION < 1.2, reason=f"Fluent pandas not supported on {PANDAS_VERSION}"
    )
]


class MockContainerClient:
    def walk_blobs(
        self,
        name_starts_with: str | None = None,
        include: Any | None = None,
        delimiter: str = "/",
        **kwargs,
    ) -> Iterator:
        return iter([])


class MockBlobServiceClient:
    def get_container_client(self, container: str) -> azure.ContainerClient:
        return cast(azure.ContainerClient, MockContainerClient())


def _build_pandas_abs_datasource(
    azure_options: Dict[str, Any] | None = None,
) -> PandasAzureBlobStorageDatasource:
    azure_client: azure.BlobServiceClient = cast(azure.BlobServiceClient, MockBlobServiceClient())
    pandas_abs_datasource = PandasAzureBlobStorageDatasource(
        name="pandas_abs_datasource",
        azure_options=azure_options or {},
    )
    pandas_abs_datasource._azure_client = azure_client
    return pandas_abs_datasource


@pytest.fixture
def pandas_abs_datasource() -> PandasAzureBlobStorageDatasource:
    pandas_abs_datasource: PandasAzureBlobStorageDatasource = _build_pandas_abs_datasource()
    return pandas_abs_datasource


@pytest.fixture
def object_keys() -> List[str]:
    return [
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
    ]


@pytest.mark.big
def test_construct_pandas_abs_datasource_with_account_url_and_credential():
    pandas_abs_datasource = PandasAzureBlobStorageDatasource(
        name="pandas_abs_datasource",
        azure_options={
            "account_url": "my_account_url.blob.core.windows.net",
            "credential": "my_credential",
        },
    )
    azure_client: azure.BlobServiceClient = pandas_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert pandas_abs_datasource.name == "pandas_abs_datasource"


@pytest.mark.big
def test_construct_pandas_abs_datasource_with_account_url_and_config_credential(
    monkeypatch: pytest.MonkeyPatch, empty_file_context: FileDataContext
):
    monkeypatch.setenv("MY_CRED", "my_secret_credential")

    pandas_abs_datasource = PandasAzureBlobStorageDatasource(
        name="pandas_abs_datasource",
        azure_options={
            "account_url": "my_account_url.blob.core.windows.net",
            "credential": r"${MY_CRED}",
        },
    )

    # attach data_context to enable config substitution
    pandas_abs_datasource._data_context = empty_file_context

    credential = pandas_abs_datasource.azure_options["credential"]
    assert isinstance(credential, ConfigStr)
    assert (
        credential.get_config_value(config_provider=empty_file_context.config_provider)
        == "my_secret_credential"
    )

    azure_client: azure.BlobServiceClient = pandas_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert pandas_abs_datasource.name == "pandas_abs_datasource"


@pytest.mark.big
def test_construct_pandas_abs_datasource_with_conn_str_and_credential():
    pandas_abs_datasource = PandasAzureBlobStorageDatasource(
        name="pandas_abs_datasource",
        azure_options={  # Representative of format noted in official docs
            "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",  # noqa: E501
            "credential": "my_credential",
        },
    )
    azure_client: azure.BlobServiceClient = pandas_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert pandas_abs_datasource.name == "pandas_abs_datasource"


@pytest.mark.big
def test_construct_pandas_abs_datasource_with_valid_account_url_assigns_account_name():
    pandas_abs_datasource = PandasAzureBlobStorageDatasource(
        name="pandas_abs_datasource",
        azure_options={
            "account_url": "my_account_url.blob.core.windows.net",
            "credential": "my_credential",
        },
    )
    azure_client: azure.BlobServiceClient = pandas_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert pandas_abs_datasource.name == "pandas_abs_datasource"


@pytest.mark.big
def test_construct_pandas_abs_datasource_with_valid_conn_str_assigns_account_name():
    pandas_abs_datasource = PandasAzureBlobStorageDatasource(
        name="pandas_abs_datasource",
        azure_options={  # Representative of format noted in official docs
            "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",  # noqa: E501
            "credential": "my_credential",
        },
    )
    azure_client: azure.BlobServiceClient = pandas_abs_datasource._get_azure_client()
    assert azure_client is not None
    assert pandas_abs_datasource.name == "pandas_abs_datasource"


@pytest.mark.big
def test_construct_pandas_abs_datasource_with_multiple_auth_methods_raises_error():
    # Raises error in DataContext's schema validation due to having both `account_url` and `conn_str`  # noqa: E501
    with pytest.raises(PandasAzureBlobStorageDatasourceError):
        pandas_abs_datasource = PandasAzureBlobStorageDatasource(
            name="pandas_abs_datasource",
            azure_options={
                "account_url": "account.blob.core.windows.net",
                "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",  # noqa: E501
                "credential": "my_credential",
            },
        )
        _ = pandas_abs_datasource._get_azure_client()


@pytest.mark.big
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_add_csv_asset_to_datasource(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    pandas_abs_datasource: PandasAzureBlobStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = pandas_abs_datasource.add_csv_asset(
        name="csv_asset",
        abs_container="my_container",
    )
    assert asset.name == "csv_asset"


@pytest.mark.big
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_construct_csv_asset_directly(mock_azure_client, mock_list_keys, object_keys: List[str]):
    mock_list_keys.return_value = object_keys
    asset = CSVAsset(  # type: ignore[call-arg]
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.big
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_csv_asset_with_batching_regex_named_parameters(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    pandas_abs_datasource: PandasAzureBlobStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    asset = pandas_abs_datasource.add_csv_asset(
        name="csv_asset",
        abs_container="my_container",
    )
    batch_def = asset.add_batch_definition_monthly(name="batch def", regex=regex)
    options = asset.get_batch_parameters_keys(partitioner=batch_def.partitioner)
    assert options == ("path", "year", "month")


@pytest.mark.big
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    pandas_abs_datasource: PandasAzureBlobStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = pandas_abs_datasource.add_csv_asset(
        name="csv_asset",
        abs_container="my_container",
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # price is an int which will raise an error
        asset.build_batch_request({"name": "alex", "timestamp": "1234567890", "price": 1300})


@pytest.mark.big
@mock.patch(
    "great_expectations.datasource.fluent.data_connector.azure_blob_storage_data_connector.list_azure_keys"
)
@mock.patch("azure.storage.blob.BlobServiceClient")
def test_add_csv_asset_with_recursive_file_discovery_to_datasource(
    mock_azure_client,
    mock_list_keys,
    object_keys: List[str],
    pandas_abs_datasource: PandasAzureBlobStorageDatasource,
):
    """
    Tests that the abs_recursive_file_discovery-flag is passed on
    to the list_keys-function as the recursive-parameter

    This makes the list_keys-function search and return files also
    from sub-directories on Azure, not just the files in the folder
    specified with the abs_name_starts_with-parameter
    """
    mock_list_keys.return_value = object_keys
    pandas_abs_datasource.add_csv_asset(
        name="csv_asset",
        abs_container="my_container",
        abs_recursive_file_discovery=True,
    )
    assert "recursive" in mock_list_keys.call_args.kwargs
    assert mock_list_keys.call_args.kwargs["recursive"] is True
