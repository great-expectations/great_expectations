import os

import pytest
from azure.storage.blob import BlobServiceClient, ContainerClient

from great_expectations import DataContext
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


def test_list_azure_keys_basic(blob_service_client: BlobServiceClient) -> None:
    keys = list_azure_keys(
        azure=blob_service_client,
        query_options={},
        container=CONTAINER_NAME,
        recursive=False,
    )
    assert keys == [
        "yellow_trip_data_sample_2018-01.csv",
        "yellow_trip_data_sample_2018-02.csv",
        "yellow_trip_data_sample_2018-03.csv",
    ]


def test_list_azure_keys_with_prefix(blob_service_client: BlobServiceClient) -> None:
    keys = list_azure_keys(
        azure=blob_service_client,
        query_options={"name_starts_with": "2018/"},
        container=CONTAINER_NAME,
        recursive=False,
    )
    assert keys == [
        "2018/yellow_trip_data_sample_2018-01.csv",
        "2018/yellow_trip_data_sample_2018-02.csv",
        "2018/yellow_trip_data_sample_2018-03.csv",
    ]


def test_list_azure_keys_with_prefix_and_recursive(
    blob_service_client: BlobServiceClient,
) -> None:
    keys = list_azure_keys(
        azure=blob_service_client,
        query_options={"name_starts_with": "2018/"},
        container=CONTAINER_NAME,
        recursive=True,
    )
    assert keys == [
        "2018/2018-04/yellow_trip_data_sample_2018-04.csv",
        "2018/2018-05/yellow_trip_data_sample_2018-05.csv",
        "2018/2018-06/yellow_trip_data_sample_2018-06.csv",
        "2018/yellow_trip_data_sample_2018-01.csv",
        "2018/yellow_trip_data_sample_2018-02.csv",
        "2018/yellow_trip_data_sample_2018-03.csv",
    ]


def test_basic_instantiation() -> None:
    my_data_connector = ConfiguredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "yellow_trip_data_sample_(.*)\\.csv",
            "group_names": ["timestamp"],
        },
        container=CONTAINER_NAME,
        name_starts_with="",
        assets={"alpha": {}},
        azure_options={"account_url": ACCOUNT_URL},
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


def test_instantiation_from_a_config(
    empty_data_context_stats_enabled, blob_service_client: BlobServiceClient
):
    context: DataContext = empty_data_context_stats_enabled

    report_object = context.test_yaml_config(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: ConfiguredAssetAzureDataConnector
        datasource_name: FAKE_DATASOURCE
        name: TEST_DATA_CONNECTOR
        default_regex:
            pattern: yellow_trip_data_sample_(.*)\\.csv
            group_names:
                - timestamp
        container: {CONTAINER_NAME}
        name_starts_with: ""
        assets:
            alpha:
        azure_options:
            account_url: {ACCOUNT_URL}
    """,
        return_mode="report_object",
    )

    assert report_object == {
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


# def test_instantiation_from_a_config_regex_does_not_match_paths(empty_data_context_stats_enabled):
#     context: DataContext = empty_data_context_stats_enabled

#     report_object = context.test_yaml_config(
#         f"""
#         module_name: great_expectations.datasource.data_connector
#         class_name: ConfiguredAssetAzureDataConnector
#         datasource_name: FAKE_DATASOURCE
#         name: TEST_DATA_CONNECTOR

#         container: {CONTAINER_NAME}
#         prefix: ""

#         default_regex:
#             pattern: yellow_trip_data_sample_(.*)\\.csv
#             group_names:
#                 - timestamp

#         assets:
#             alpha:
#     """,
#         return_mode="report_object",
#     )

# def test_return_all_batch_definitions_unsorted():
#     raise NotImplementedError()

# def test_return_all_batch_definitions_sorted():
#     raise NotImplementedError()

# def test_return_all_batch_definitions_sorted_sorter_named_that_does_not_match_group():
#     raise NotImplementedError()

# def test_return_all_batch_definitions_too_many_sorters():
#     raise NotImplementedError()

# def test_example_with_explicit_data_asset_names():
#     raise NotImplementedError()
