from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from great_expectations.core.data_context_key import StringKey
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import ValidationDefinitionStore
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.ephemeral_data_context import (
        EphemeralDataContext,
    )
    from tests.datasource.fluent._fake_cloud_api import CloudDetails


@pytest.fixture
def ephemeral_store():
    return ValidationDefinitionStore(store_name="ephemeral_validation_definition_store")


@pytest.fixture
def file_backed_store(tmp_path):
    base_directory = tmp_path / "base_dir"
    return ValidationDefinitionStore(
        store_name="file_backed_validation_definition_store",
        store_backend={
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": base_directory,
        },
    )


@pytest.fixture
def cloud_backed_store(cloud_details: CloudDetails):
    return ValidationDefinitionStore(
        store_name="cloud_backed_validation_definition_store",
        store_backend={
            "class_name": "GXCloudStoreBackend",
            "ge_cloud_resource_type": GXCloudRESTResource.VALIDATION_DEFINITION,
            "ge_cloud_credentials": {
                "access_token": cloud_details.access_token,
                "organization_id": cloud_details.org_id,
            },
        },
    )


@pytest.fixture
def validation_definition(
    in_memory_runtime_context: EphemeralDataContext,
) -> ValidationDefinition:
    context = in_memory_runtime_context
    batch_definition = (
        context.data_sources.add_pandas("my_datasource")
        .add_csv_asset("my_asset", "data.csv")  # type: ignore[arg-type]
        .add_batch_definition("my_batch_definition")
    )
    return ValidationDefinition(
        name="my_validation",
        data=batch_definition,
        suite=context.suites.add(ExpectationSuite(name="my_suite")),
    )


@pytest.mark.parametrize("store_fixture", ["ephemeral_store", "file_backed_store"])
@pytest.mark.unit
def test_add(request, store_fixture: str, validation_definition: ValidationDefinition):
    store: ValidationDefinitionStore = request.getfixturevalue(store_fixture)

    key = StringKey(key="my_validation")

    assert not validation_definition.id
    store.add(key=key, value=validation_definition)
    assert validation_definition.id

    assert store.get(key) == validation_definition


@pytest.mark.cloud
def test_add_cloud(
    cloud_backed_store: ValidationDefinitionStore, validation_definition: ValidationDefinition
):
    store = cloud_backed_store

    id = "5a8ada9f-5b71-461b-b1af-f1d93602a156"
    name = "my_validation"
    key = GXCloudIdentifier(
        resource_type=GXCloudRESTResource.VALIDATION_DEFINITION,
        id=id,
        resource_name=name,
    )

    with mock.patch("requests.Session.put", autospec=True) as mock_put:
        store.add(key=key, value=validation_definition)

    mock_put.assert_called_once_with(
        mock.ANY,  # requests Session
        f"https://api.greatexpectations.io/api/v1/organizations/12345678-1234-5678-1234-567812345678/validation-definitions/{id}",
        json={
            "data": {
                "name": name,
                "data": {
                    # NOTE: IDs here are unrealistic because the validation_definition
                    # was created via the in-memory store
                    "batch_definition": {"id": mock.ANY, "name": "my_batch_definition"},
                    "asset": {"id": mock.ANY, "name": "my_asset"},
                    "datasource": {"id": mock.ANY, "name": "my_datasource"},
                },
                "suite": {
                    "name": "my_suite",
                    "id": mock.ANY,
                },
            },
        },
    )


@pytest.mark.parametrize("store_fixture", ["ephemeral_store", "file_backed_store"])
@pytest.mark.unit
def test_get_key(request, store_fixture: str):
    store: ValidationDefinitionStore = request.getfixturevalue(store_fixture)

    name = "my_validation"
    assert store.get_key(name=name) == StringKey(key=name)


@pytest.mark.cloud
def test_get_key_cloud(cloud_backed_store: ValidationDefinitionStore):
    key = cloud_backed_store.get_key(name="my_validation")
    assert key.resource_type == GXCloudRESTResource.VALIDATION_DEFINITION  # type: ignore[union-attr]
    assert key.resource_name == "my_validation"  # type: ignore[union-attr]


_VALIDATION_ID = "a4sdfd-64c8-46cb-8f7e-03c12cea1d67"
_VALIDATION_DEFINITION = {
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
        "batch_definition": {
            "name": "my_batch_definition",
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
                    **_VALIDATION_DEFINITION,
                    "id": _VALIDATION_ID,
                }
            },
            id="single_validation_definition",
        ),
        pytest.param(
            {
                "data": [
                    {
                        **_VALIDATION_DEFINITION,
                        "id": _VALIDATION_ID,
                    }
                ]
            },
            id="list_with_single_validation_definition",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict_success(response_json: dict):
    actual = ValidationDefinitionStore.gx_cloud_response_json_to_object_dict(response_json)
    expected = {**_VALIDATION_DEFINITION, "id": _VALIDATION_ID}
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
                            "validation_definition": _VALIDATION_DEFINITION,
                        },
                    },
                    {
                        "id": _VALIDATION_ID,
                        "attributes": {
                            "validation_definition": _VALIDATION_DEFINITION,
                        },
                    },
                ],
            },
            "Cannot parse multiple items",
            id="list_with_multiple_validation_definitions",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict_failure(response_json: dict, error_substring: str):
    with pytest.raises(ValueError, match=f"{error_substring}*."):
        ValidationDefinitionStore.gx_cloud_response_json_to_object_dict(response_json)
