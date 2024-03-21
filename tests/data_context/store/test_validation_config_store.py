from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from great_expectations.core.data_context_key import StringKey
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import ValidationConfigStore
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.ephemeral_data_context import (
        EphemeralDataContext,
    )
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
def validation_config(
    in_memory_runtime_context: EphemeralDataContext,
) -> ValidationConfig:
    context = in_memory_runtime_context
    batch_config = (
        context.sources.add_pandas("my_datasource")
        .add_csv_asset("my_asset", "data.csv")
        .add_batch_config("my_batch_config")
    )
    return ValidationConfig(
        name="my_validation",
        data=batch_config,
        suite=ExpectationSuite(name="my_suite"),
    )


@pytest.mark.parametrize("store_fixture", ["ephemeral_store", "file_backed_store"])
@pytest.mark.unit
def test_add(request, store_fixture: str, validation_config: ValidationConfig):
    store: ValidationConfigStore = request.getfixturevalue(store_fixture)

    key = StringKey(key="my_validation")

    assert not validation_config.id
    store.add(key=key, value=validation_config)
    assert validation_config.id

    assert store.get(key) == validation_config


@pytest.mark.cloud
def test_add_cloud(cloud_backed_store: ValidationConfigStore, validation_config: ValidationConfig):
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
                        "id": None,
                        "data": {
                            "id": None,
                            "name": "my_batch_config",
                            "partitioner": None,
                        },
                        "suite": {
                            "name": "my_suite",
                            "id": None,
                            "expectations": [],
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


_VALIDATION_ID = "a4sdfd-64c8-46cb-8f7e-03c12cea1d67"
_VALIDATION_CONFIG = {
    "name": "my_validation",
    "data": {
        "datasource": {
            "name": "my_datasource",
            "id": "a758816-64c8-46cb-8f7e-03c12cea1d67",
        },
        "asset": {
            "name": "my_asset",
            "id": "b5s8816-64c8-46cb-8f7e-03c12cea1d67",
        },
        "batch_config": {
            "name": "my_batch_config",
            "id": "3a758816-64c8-46cb-8f7e-03c12cea1d67",
        },
    },
    "suite": {
        "name": "my_suite",
        "id": "8r2g816-64c8-46cb-8f7e-03c12cea1d67",
    },
}


@pytest.mark.cloud
@pytest.mark.parametrize(
    "response_json",
    [
        pytest.param(
            {
                "data": {
                    "id": _VALIDATION_ID,
                    "attributes": {
                        "validation_config": _VALIDATION_CONFIG,
                    },
                }
            },
            id="single_validation_config",
        ),
        pytest.param(
            {
                "data": [
                    {
                        "id": _VALIDATION_ID,
                        "attributes": {
                            "validation_config": _VALIDATION_CONFIG,
                        },
                    }
                ]
            },
            id="list_with_single_validation_config",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict_success(response_json: dict):
    actual = ValidationConfigStore.gx_cloud_response_json_to_object_dict(response_json)
    expected = {**_VALIDATION_CONFIG, "id": _VALIDATION_ID}
    assert actual == expected


@pytest.mark.cloud
@pytest.mark.parametrize(
    "response_json, error_substring",
    [
        pytest.param(
            {
                "data": [],
            },
            "Cannot parse empty data",
            id="empty_list",
        ),
        pytest.param(
            {
                "data": [
                    {
                        "id": _VALIDATION_ID,
                        "attributes": {
                            "validation_config": _VALIDATION_CONFIG,
                        },
                    },
                    {
                        "id": _VALIDATION_ID,
                        "attributes": {
                            "validation_config": _VALIDATION_CONFIG,
                        },
                    },
                ],
            },
            "Cannot parse multiple items",
            id="list_with_multiple_validation_configs",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict_failure(response_json: dict, error_substring: str):
    with pytest.raises(ValueError, match=f"{error_substring}*."):
        ValidationConfigStore.gx_cloud_response_json_to_object_dict(response_json)
