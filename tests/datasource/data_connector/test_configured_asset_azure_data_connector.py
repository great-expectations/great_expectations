import pytest
from azure.storage.blob import ContainerClient

from great_expectations.datasource.data_connector.util import list_azure_keys


@pytest.fixture
def container():
    container = ContainerClient(
        container_name="<INSERT_CONTAINER_NAME_HERE>",
        account_url="<INSERT_ACCOUNT_URL_HERE>",
    )
    return container


def test_container_lists_blobs(container):
    blobs = [blob for blob in container.list_blobs()]
    assert len(blobs) == 6


def test_list_azure_keys(container):
    pass
