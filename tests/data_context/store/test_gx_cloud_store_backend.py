"""
Test GXCloudStoreBackend behavior and adherence to StoreBackend contract.

Since GXCloudStoreBackend relies on GX Cloud, we mock requests and assert the right calls
are made from the Store API (set, get, list, and remove_key).

Note that although ge_cloud_access_token is provided (and is a valid UUID), no external
requests are actually made as part of this test. The actual value of the token does not
matter here but we leverage an existing fixture to mimic the contents of requests made
in production. The same logic applies to all UUIDs in this test.
"""

from collections import OrderedDict
from typing import Callable, Optional, Set, Union
from unittest import mock

import pytest

from great_expectations.data_context.cloud_constants import (
    CLOUD_DEFAULT_BASE_URL,
    GXCloudRESTResource,
)
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
    construct_json_payload,
    construct_url,
)
from great_expectations.data_context.types.base import CheckpointConfig


@pytest.fixture
def construct_ge_cloud_store_backend(
    ge_cloud_access_token: str,
) -> Callable[[GXCloudRESTResource], GXCloudStoreBackend]:
    def _closure(resource_type: GXCloudRESTResource) -> GXCloudStoreBackend:
        ge_cloud_base_url = CLOUD_DEFAULT_BASE_URL
        ge_cloud_credentials = {
            "access_token": ge_cloud_access_token,
            "organization_id": "51379b8b-86d3-4fe7-84e9-e1a52f4a414c",
        }

        store_backend = GXCloudStoreBackend(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_resource_type=resource_type,
        )
        return store_backend

    return _closure


@pytest.mark.parametrize(
    "base_url,organization_id,resource_name,id,expected",
    [
        pytest.param(
            "https://app.test.greatexpectations.io",
            "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
            "my_resource",
            None,
            "https://app.test.greatexpectations.io/organizations/de5b9ca6-caf7-43c8-a820-5540ec6df9b2/my-resource",
            id="no id",
        ),
        pytest.param(
            "https://app.test.greatexpectations.io",
            "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
            "my_resource",
            "8746e25c-de4d-450d-967b-df0d5546590d",
            "https://app.test.greatexpectations.io/organizations/de5b9ca6-caf7-43c8-a820-5540ec6df9b2/my-resource/8746e25c-de4d-450d-967b-df0d5546590d",
            id="with id",
        ),
    ],
)
@pytest.mark.unit
@pytest.mark.cloud
def test_construct_url(
    base_url: str,
    organization_id: str,
    resource_name: str,
    id: Optional[str],
    expected: str,
) -> None:
    assert (
        construct_url(
            base_url=base_url,
            organization_id=organization_id,
            resource_name=resource_name,
            id=id,
        )
        == expected
    )


@pytest.mark.parametrize(
    "resource_type,organization_id,attributes_key,attributes_value,kwargs,expected",
    [
        pytest.param(
            "my_resource",
            "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
            "my_attribute",
            "my_value",
            {},
            {
                "data": {
                    "type": "my_resource",
                    "attributes": {
                        "organization_id": "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
                        "my_attribute": "my_value",
                    },
                },
            },
            id="no kwargs",
        ),
        pytest.param(
            "my_resource",
            "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
            "my_attribute",
            {"key1": {"nested_key1": 1}, "key2": {"nested_key2": 2}},
            {},
            {
                "data": {
                    "type": "my_resource",
                    "attributes": {
                        "organization_id": "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
                        "my_attribute": {
                            "key1": {"nested_key1": 1},
                            "key2": {"nested_key2": 2},
                        },
                    },
                },
            },
            id="with nested value",
        ),
        pytest.param(
            "my_resource",
            "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
            "my_attribute",
            "my_value",
            {
                "kwarg1": 1,
                "kwarg2": 2,
            },
            {
                "data": {
                    "type": "my_resource",
                    "attributes": {
                        "organization_id": "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
                        "my_attribute": "my_value",
                        "kwarg1": 1,
                        "kwarg2": 2,
                    },
                },
            },
            id="with basic kwargs",
        ),
        pytest.param(
            "my_resource",
            "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
            "my_attribute",
            "my_value",
            {
                "kwarg1": {
                    "nested1": 1,
                },
                "kwarg2": {
                    "nested2": 2,
                },
            },
            {
                "data": {
                    "type": "my_resource",
                    "attributes": {
                        "organization_id": "de5b9ca6-caf7-43c8-a820-5540ec6df9b2",
                        "my_attribute": "my_value",
                        "kwarg1": {
                            "nested1": 1,
                        },
                        "kwarg2": {
                            "nested2": 2,
                        },
                    },
                },
            },
            id="with nested kwargs",
        ),
    ],
)
@pytest.mark.unit
@pytest.mark.cloud
def test_construct_json_payload(
    resource_type: str,
    organization_id: str,
    attributes_key: str,
    attributes_value: str,
    kwargs: dict,
    expected: dict,
) -> None:
    assert (
        construct_json_payload(
            resource_type=resource_type,
            organization_id=organization_id,
            attributes_key=attributes_key,
            attributes_value=attributes_value,
            **kwargs,
        )
        == expected
    )


@pytest.mark.cloud
@pytest.mark.unit
def test_set(
    construct_ge_cloud_store_backend: Callable[
        [GXCloudRESTResource], GXCloudStoreBackend
    ],
) -> None:
    store_backend = construct_ge_cloud_store_backend(GXCloudRESTResource.CHECKPOINT)

    my_simple_checkpoint_config: CheckpointConfig = CheckpointConfig(
        name="my_minimal_simple_checkpoint",
        class_name="SimpleCheckpoint",
        config_version=1,
    )
    my_simple_checkpoint_config_serialized = (
        my_simple_checkpoint_config.get_schema_class()().dump(
            my_simple_checkpoint_config
        )
    )

    with mock.patch("requests.Session.post", autospec=True) as mock_post:
        store_backend.set(("checkpoint", ""), my_simple_checkpoint_config_serialized)
        mock_post.assert_called_with(
            mock.ANY,  # requests.Session object
            f"{CLOUD_DEFAULT_BASE_URL}organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/checkpoints",
            json={
                "data": {
                    "type": "checkpoint",
                    "attributes": {
                        "organization_id": "51379b8b-86d3-4fe7-84e9-e1a52f4a414c",
                        "checkpoint_config": OrderedDict(
                            [
                                ("name", "my_minimal_simple_checkpoint"),
                                ("config_version", 1.0),
                                ("template_name", None),
                                ("module_name", "great_expectations.checkpoint"),
                                ("class_name", "SimpleCheckpoint"),
                                ("run_name_template", None),
                                ("expectation_suite_name", None),
                                ("batch_request", {}),
                                ("action_list", []),
                                ("evaluation_parameters", {}),
                                ("runtime_configuration", {}),
                                ("validations", []),
                                ("profilers", []),
                                ("ge_cloud_id", None),
                                ("expectation_suite_ge_cloud_id", None),
                            ]
                        ),
                    },
                }
            },
        )


@pytest.mark.cloud
@pytest.mark.unit
def test_list_keys(
    construct_ge_cloud_store_backend: Callable[
        [GXCloudRESTResource], GXCloudStoreBackend
    ],
) -> None:
    store_backend = construct_ge_cloud_store_backend(GXCloudRESTResource.CHECKPOINT)

    with mock.patch("requests.Session.get", autospec=True) as mock_get:
        store_backend.list_keys()
        mock_get.assert_called_with(
            mock.ANY,  # requests.Session object
            f"{CLOUD_DEFAULT_BASE_URL}organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/checkpoints",
        )


@pytest.mark.cloud
@pytest.mark.unit
def test_remove_key(
    construct_ge_cloud_store_backend: Callable[
        [GXCloudRESTResource], GXCloudStoreBackend
    ],
) -> None:
    store_backend = construct_ge_cloud_store_backend(GXCloudRESTResource.CHECKPOINT)

    with mock.patch("requests.Session.delete", autospec=True) as mock_delete:
        mock_response = mock_delete.return_value
        mock_response.status_code = 200

        store_backend.remove_key(
            (
                "checkpoint",
                "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
            )
        )
        mock_delete.assert_called_with(
            mock.ANY,  # requests.Session object
            f"{CLOUD_DEFAULT_BASE_URL}organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/checkpoints/0ccac18e-7631"
            "-4bdd"
            "-8a42-3c35cce574c6",
            json={
                "data": {
                    "type": "checkpoint",
                    "id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    "attributes": {"deleted": True},
                }
            },
        )


@pytest.mark.cloud
@pytest.mark.unit
def test_appropriate_casting_of_str_resource_type_to_GXCloudRESTResource(
    construct_ge_cloud_store_backend: Callable[
        [GXCloudRESTResource], GXCloudStoreBackend
    ],
) -> None:
    store_backend = construct_ge_cloud_store_backend(GXCloudRESTResource.CHECKPOINT)

    assert store_backend.ge_cloud_resource_type is GXCloudRESTResource.CHECKPOINT


@pytest.mark.parametrize(
    "resource_type,expected_set_kwargs",
    [
        pytest.param(
            GXCloudRESTResource.VALIDATION_RESULT,
            {"checkpoint_id", "expectation_suite_id"},
            id="resource_with_allowed_set_kwargs",
        ),
        pytest.param(
            GXCloudRESTResource.CHECKPOINT,
            set(),
            id="resource_without_allowed_set_kwargs",
        ),
    ],
)
@pytest.mark.cloud
@pytest.mark.unit
def test_allowed_set_kwargs(
    resource_type: GXCloudRESTResource,
    expected_set_kwargs: Set[str],
    construct_ge_cloud_store_backend: Callable[
        [GXCloudRESTResource], GXCloudStoreBackend
    ],
) -> None:
    store_backend = construct_ge_cloud_store_backend(resource_type)
    actual = store_backend.allowed_set_kwargs

    assert actual == expected_set_kwargs


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        pytest.param(
            {},
            True,
            id="no kwargs",
        ),
        pytest.param(
            {"checkpoint_id": "123", "expectation_suite_id": "456"},
            True,
            id="equivalent kwargs",
        ),
        pytest.param(
            {
                "checkpoint_id": "123",
                "expectation_suite_id": "456",
                "my_other_id": "789",
            },
            None,
            id="additional kwargs",
            marks=pytest.mark.xfail(strict=True, raises=ValueError),
        ),
    ],
)
@pytest.mark.cloud
@pytest.mark.unit
def test_validate_set_kwargs(
    kwargs: dict,
    expected: Union[bool, None],
    construct_ge_cloud_store_backend: Callable[
        [GXCloudRESTResource], GXCloudStoreBackend
    ],
) -> None:
    store_backend = construct_ge_cloud_store_backend(
        GXCloudRESTResource.VALIDATION_RESULT
    )
    store_backend.validate_set_kwargs(kwargs) == expected


@pytest.mark.cloud
@pytest.mark.unit
def test_config_property_and_defaults(
    construct_ge_cloud_store_backend: Callable[
        [GXCloudRESTResource], GXCloudStoreBackend
    ],
) -> None:
    store_backend = construct_ge_cloud_store_backend(GXCloudRESTResource.CHECKPOINT)

    assert store_backend.config == {
        "class_name": GXCloudStoreBackend.__name__,
        "fixed_length_key": True,
        "ge_cloud_base_url": CLOUD_DEFAULT_BASE_URL,
        "ge_cloud_resource_type": GXCloudRESTResource.CHECKPOINT,
        "manually_initialize_store_backend_id": "",
        "module_name": GXCloudStoreBackend.__module__,
        "suppress_store_backend_id": True,
    }
