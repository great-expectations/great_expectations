import os

import pytest
from azure.storage.blob import BlobPrefix, BlobServiceClient, ContainerClient

from great_expectations.datasource.data_connector import (
    ConfiguredAssetAzureDataConnector,
)
from great_expectations.datasource.data_connector.util import list_azure_keys

try:
    ACCOUNT_URL = os.environ["ACCOUNT_URL"]
    CONTAINER_NAME = os.environ["CONTAINER_NAME"]
except:
    print("Please set environment variables before running tests")


@pytest.fixture
def blob_service_client() -> BlobServiceClient:
    azure = BlobServiceClient(ACCOUNT_URL)
    return azure


@pytest.fixture
def container_client(blob_service_client: BlobServiceClient) -> ContainerClient:
    container = blob_service_client.get_container_client(CONTAINER_NAME)
    return container


def test_list_azure_keys_not_recursive(blob_service_client: BlobServiceClient) -> None:
    keys = list_azure_keys(
        azure=blob_service_client,
        query_options={"name_starts_with": ""},
        container=CONTAINER_NAME,
        recursive=False,
    )
    assert keys == [
        "yellow_trip_data_sample_2018-01.csv",
        "yellow_trip_data_sample_2018-02.csv",
        "yellow_trip_data_sample_2018-03.csv",
    ]


def test_list_azure_keys_recursive(blob_service_client: BlobServiceClient) -> None:
    keys = list_azure_keys(
        azure=blob_service_client,
        query_options={"name_starts_with": ""},
        container=CONTAINER_NAME,
        recursive=True,
    )
    assert keys == [
        "2018/yellow_trip_data_sample_2018-01.csv",
        "2018/yellow_trip_data_sample_2018-02.csv",
        "2018/yellow_trip_data_sample_2018-03.csv",
        "yellow_trip_data_sample_2018-01.csv",
        "yellow_trip_data_sample_2018-02.csv",
        "yellow_trip_data_sample_2018-03.csv",
    ]


def test_self_check():
    my_data_connector = ConfiguredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "yellow_trip_data_sample_(.*)\\.csv",
            "group_names": ["timestamp"],
        },
        container=CONTAINER_NAME,
        prefix="",
        assets={"alpha": {}},
    )

    assert my_data_connector.self_check() == {
        "class_name": "ConfiguredAssetAzureDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "yellow_trip_data_sample_2018-01.csv",
                    "yellow_trip_data_sample_2018-02.csv",
                    "yellow_trip_data_sample_2018-03.csv",
                ],
                "batch_definition_count": 3,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }
