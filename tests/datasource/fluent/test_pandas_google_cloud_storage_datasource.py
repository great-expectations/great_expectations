from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterator, List, cast
from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.compatibility import google
from great_expectations.datasource.fluent import (
    PandasGoogleCloudStorageDatasource,
)
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import CSVAsset
from great_expectations.datasource.fluent.dynamic_pandas import PANDAS_VERSION

logger = logging.getLogger(__file__)


if not google.storage:
    pytest.skip(
        'Could not import "storage" from google.cloud in configured_asset_gcs_data_connector.py',
        allow_module_level=True,
    )


# apply markers to entire test module
pytestmark = [
    pytest.mark.skipif(
        PANDAS_VERSION < 1.2, reason=f"Fluent pandas not supported on {PANDAS_VERSION}"
    )
]


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


def _build_pandas_gcs_datasource(
    gcs_options: Dict[str, Any] | None = None,
) -> PandasGoogleCloudStorageDatasource:
    gcs_client: google.Client = cast(google.Client, MockGCSClient())
    pandas_gcs_datasource = PandasGoogleCloudStorageDatasource(
        name="pandas_gcs_datasource",
        bucket_or_name="test_bucket",
        gcs_options=gcs_options or {},
    )
    pandas_gcs_datasource._gcs_client = gcs_client
    return pandas_gcs_datasource


@pytest.fixture
def pandas_gcs_datasource() -> PandasGoogleCloudStorageDatasource:
    pandas_gcs_datasource: PandasGoogleCloudStorageDatasource = _build_pandas_gcs_datasource()
    return pandas_gcs_datasource


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


@pytest.mark.unit
def test_construct_pandas_gcs_datasource_without_gcs_options():
    google_cred_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not google_cred_file:
        pytest.skip('No "GOOGLE_APPLICATION_CREDENTIALS" environment variable found.')

    pandas_gcs_datasource = PandasGoogleCloudStorageDatasource(
        name="pandas_gcs_datasource",
        bucket_or_name="test_bucket",
        gcs_options={},
    )
    gcs_client: google.Client = pandas_gcs_datasource._get_gcs_client()
    assert gcs_client is not None
    assert pandas_gcs_datasource.name == "pandas_gcs_datasource"


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.oauth2.service_account.Credentials.from_service_account_file")
@mock.patch("google.cloud.storage.Client")
def test_construct_pandas_gcs_datasource_with_filename_in_gcs_options(
    mock_gcs_client, mock_gcs_service_account_credentials, mock_list_keys
):
    pandas_gcs_datasource = PandasGoogleCloudStorageDatasource(
        name="pandas_gcs_datasource",
        bucket_or_name="test_bucket",
        gcs_options={
            "filename": "my_filename.csv",
        },
    )
    gcs_client: google.Client = pandas_gcs_datasource._get_gcs_client()
    assert gcs_client is not None
    assert pandas_gcs_datasource.name == "pandas_gcs_datasource"


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.oauth2.service_account.Credentials.from_service_account_info")
@mock.patch("google.cloud.storage.Client")
def test_construct_pandas_gcs_datasource_with_info_in_gcs_options(
    mock_gcs_client, mock_gcs_service_account_credentials, mock_list_keys
):
    pandas_gcs_datasource = PandasGoogleCloudStorageDatasource(
        name="pandas_gcs_datasource",
        bucket_or_name="test_bucket",
        gcs_options={
            "info": "{my_csv: my_content,}",
        },
    )
    gcs_client: google.Client = pandas_gcs_datasource._get_gcs_client()
    assert gcs_client is not None
    assert pandas_gcs_datasource.name == "pandas_gcs_datasource"


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_add_csv_asset_to_datasource(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    pandas_gcs_datasource: PandasGoogleCloudStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = pandas_gcs_datasource.add_csv_asset(
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_construct_csv_asset_directly(mock_gcs_client, mock_list_keys, object_keys: List[str]):
    mock_list_keys.return_value = object_keys
    asset = CSVAsset(  # type: ignore[call-arg]
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_csv_asset_with_batching_regex_named_parameters(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    pandas_gcs_datasource: PandasGoogleCloudStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = pandas_gcs_datasource.add_csv_asset(
        name="csv_asset",
    )
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"

    batch_def = asset.add_batch_definition_monthly(name="batch def", regex=batching_regex)
    options = asset.get_batch_parameters_keys(partitioner=batch_def.partitioner)
    assert options == ("path", "year", "month")


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    pandas_gcs_datasource: PandasGoogleCloudStorageDatasource,
):
    mock_list_keys.return_value = object_keys
    asset = pandas_gcs_datasource.add_csv_asset(
        name="csv_asset",
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # price is an int which will raise an error
        asset.build_batch_request({"name": "alex", "timestamp": "1234567890", "price": 1300})


@pytest.mark.unit
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
@mock.patch("google.cloud.storage.Client")
def test_add_csv_asset_with_recursive_file_discovery_to_datasource(
    mock_gcs_client,
    mock_list_keys,
    object_keys: List[str],
    pandas_gcs_datasource: PandasGoogleCloudStorageDatasource,
):
    """
    Tests that the gcs_recursive_file_discovery-flag is passed on
    to the list_keys-function as the recursive-parameter

    This makes the list_keys-function search and return files also
    from sub-directories on GCS, not just the files in the folder
    specified with the abs_name_starts_with-parameter
    """
    mock_list_keys.return_value = object_keys
    pandas_gcs_datasource.add_csv_asset(
        name="csv_asset",
        gcs_recursive_file_discovery=True,
    )
    assert "recursive" in mock_list_keys.call_args.kwargs
    assert mock_list_keys.call_args.kwargs["recursive"] is True
