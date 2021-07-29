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


def test_container_connects(container_client: ContainerClient) -> None:
    blobs = [blob for blob in container_client.list_blobs()]
    assert len(blobs) == 6


def test_list_azure_keys(blob_service_client: BlobServiceClient) -> None:
    a = list_azure_keys(
        azure=blob_service_client,
        query_options={},
        container=CONTAINER_NAME,
        iterator_dict={},
        recursive=False,
    )
    assert False
