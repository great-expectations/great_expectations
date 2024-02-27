from typing import TYPE_CHECKING
from unittest import mock

import pytest

from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.data_context_key import StringKey
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import ValidationConfigStore
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier

if TYPE_CHECKING:
    from tests.datasource.fluent._fake_cloud_api import CloudDetails


@pytest.fixture
def ephemeral_store():
    return ValidationConfigStore(store_name="ephemeral_validation_config_store")


@pytest.fixture
def file_backed_store(tmp_path):
    base_directory = tmp_path / "base_dir"
    return ValidationConfigStore(
        store_name="file_backed_validation_config_store",
        store_backend={
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": base_directory,
        },
    )


@pytest.fixture
def cloud_backed_store(cloud_details: CloudDetails):
    return ValidationConfigStore(
        store_name="cloud_backed_validation_config_store",
        store_backend={
            "class_name": "GXCloudStoreBackend",
            "ge_cloud_resource_type": GXCloudRESTResource.VALIDATION_CONFIG,
            "ge_cloud_credentials": {
                "access_token": cloud_details.access_token,
                "organization_id": cloud_details.org_id,
            },
        },
    )


@pytest.fixture
def validation_config() -> ValidationConfig:
    return ValidationConfig(
        name="my_validation",
        data=BatchConfig(name="my_batch_config"),
        suite=ExpectationSuite(name="my_suite"),
    )


@pytest.mark.parametrize("store_fixture", ["ephemeral_store", "file_backed_store"])
@pytest.mark.unit
def test_add(request, store_fixture: str, validation_config: ValidationConfig):
    store: ValidationConfigStore = request.getfixturevalue(store_fixture)

    key = StringKey(key="my_validation")
    store.add(key=key, value=validation_config)

    assert store.get(key) == validation_config


@pytest.mark.cloud
def test_add_cloud(
    cloud_backed_store: ValidationConfigStore, validation_config: ValidationConfig
):
    store = cloud_backed_store

    id = "5a8ada9f-5b71-461b-b1af-f1d93602a156"
    name = "my_validation"
    key = GXCloudIdentifier(
        resource_type=GXCloudRESTResource.VALIDATION_CONFIG,
        id=id,
        resource_name=name,
    )

    with mock.patch("requests.Session.put", autospec=True) as mock_put:
        store.add(key=key, value=validation_config)

    mock_put.assert_called_once_with(
        mock.ANY,  # requests Session
        f"https://api.greatexpectations.io/organizations/12345678-1234-5678-1234-567812345678/validation-configs/{id}",
        json={
            "data": {
                "type": "validation_config",
                "id": id,
                "attributes": {
                    "organization_id": "12345678-1234-5678-1234-567812345678",
                    "validation_config": {
                        "name": name,
                        "data": {
                            "id": None,
                            "name": "my_batch_config",
                            "partitioner": None,
                        },
                        "suite": {
                            "expectation_suite_name": "my_suite",
                            "id": None,
                            "expectations": [],
                            "data_asset_type": None,
                            "meta": mock.ANY,  # GX version information
                            "notes": None,
                        },
                    },
                },
            }
        },
    )


@pytest.mark.parametrize("store_fixture", ["ephemeral_store", "file_backed_store"])
@pytest.mark.unit
def test_get_key(request, store_fixture: str):
    store: ValidationConfigStore = request.getfixturevalue(store_fixture)

    name = "my_validation"
    assert store.get_key(name=name) == StringKey(key=name)


@pytest.mark.cloud
def test_get_key_cloud(cloud_backed_store: ValidationConfigStore):
    key = cloud_backed_store.get_key(name="my_validation")
    assert key.resource_type == GXCloudRESTResource.VALIDATION_CONFIG
    assert key.resource_name == "my_validation"
