from collections import OrderedDict
from unittest.mock import patch

import pytest

from great_expectations.data_context.store import GeCloudStoreBackend
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import CheckpointConfig


def test_GeCloudStoreBackend(
    shared_called_with_request_kwargs: dict, ge_cloud_access_token: str
):
    """
    What does this test test and why?

    Since GeCloudStoreBackend relies on GE Cloud, we mock requests.post, requests.get, and
    requests.patch and assert that the right calls are made for set, get, list, and remove_key.
    """
    ge_cloud_base_url = "https://app.greatexpectations.io/"
    ge_cloud_credentials = {
        "access_token": ge_cloud_access_token,
        "organization_id": "51379b8b-86d3-4fe7-84e9-e1a52f4a414c",
    }
    ge_cloud_resource_type = GeCloudRESTResource.CHECKPOINT
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

    # test .set
    with patch("requests.post", autospec=True) as mock_post:
        my_store_backend = GeCloudStoreBackend(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_resource_type=ge_cloud_resource_type,
        )
        my_store_backend.set(("checkpoint", ""), my_simple_checkpoint_config_serialized)
        mock_post.assert_called_with(
            "https://app.greatexpectations.io/organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/checkpoints",
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
            **shared_called_with_request_kwargs,
        )

    # test .get
    with patch("requests.get", autospec=True) as mock_get:
        my_store_backend = GeCloudStoreBackend(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_resource_type=ge_cloud_resource_type,
        )
        my_store_backend.get(
            (
                "checkpoint",
                "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
            )
        )
        mock_get.assert_called_with(
            "https://app.greatexpectations.io/organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/checkpoints/0ccac18e-7631"
            "-4bdd-8a42-3c35cce574c6",
            params=None,
            **shared_called_with_request_kwargs,
        )

    # test .list_keys
    with patch("requests.get", autospec=True) as mock_get:
        my_store_backend = GeCloudStoreBackend(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_resource_type=ge_cloud_resource_type,
        )
        my_store_backend.list_keys()
        mock_get.assert_called_with(
            "https://app.greatexpectations.io/organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/checkpoints",
            **shared_called_with_request_kwargs,
        )

    # test .remove_key
    with patch("requests.delete", autospec=True) as mock_delete:
        mock_response = mock_delete.return_value
        mock_response.status_code = 200

        my_store_backend = GeCloudStoreBackend(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_resource_type=ge_cloud_resource_type,
        )
        my_store_backend.remove_key(
            (
                "checkpoint",
                "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
            )
        )
        mock_delete.assert_called_with(
            "https://app.greatexpectations.io/organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/checkpoints/0ccac18e-7631"
            "-4bdd"
            "-8a42-3c35cce574c6",
            json={
                "data": {
                    "type": "checkpoint",
                    "id_": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    "attributes": {"deleted": True},
                }
            },
            **shared_called_with_request_kwargs,
        )

    # test .set
    with patch("requests.post", autospec=True) as mock_post:
        my_store_backend = GeCloudStoreBackend(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_resource_type=GeCloudRESTResource.RENDERED_DATA_DOC,
        )
        my_store_backend.set(("rendered_data_doc", ""), OrderedDict())
        mock_post.assert_called_with(
            "https://app.greatexpectations.io/organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/rendered-data-docs",
            json={
                "data": {
                    "type": "rendered_data_doc",
                    "attributes": {
                        "organization_id": "51379b8b-86d3-4fe7-84e9-e1a52f4a414c",
                        "rendered_data_doc": OrderedDict(),
                    },
                }
            },
            **shared_called_with_request_kwargs,
        )

    # test .get
    with patch("requests.get", autospec=True) as mock_get:
        my_store_backend = GeCloudStoreBackend(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_resource_type=GeCloudRESTResource.RENDERED_DATA_DOC,
        )
        my_store_backend.get(
            (
                "rendered_data_doc",
                "1ccac18e-7631-4bdd-8a42-3c35cce574c6",
            )
        )
        mock_get.assert_called_with(
            "https://app.greatexpectations.io/organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/rendered-data-docs/1ccac18e-7631"
            "-4bdd-8a42-3c35cce574c6",
            params=None,
            **shared_called_with_request_kwargs,
        )

    # test .list_keys
    with patch("requests.get", autospec=True) as mock_get:
        my_store_backend = GeCloudStoreBackend(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_resource_type=GeCloudRESTResource.RENDERED_DATA_DOC,
        )
        my_store_backend.list_keys()
        mock_get.assert_called_with(
            "https://app.greatexpectations.io/organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/rendered-data-docs",
            **shared_called_with_request_kwargs,
        )

    # test .remove_key
    with patch("requests.delete", autospec=True) as mock_delete:
        mock_response = mock_delete.return_value
        mock_response.status_code = 200

        my_store_backend = GeCloudStoreBackend(
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_credentials=ge_cloud_credentials,
            ge_cloud_resource_type=GeCloudRESTResource.RENDERED_DATA_DOC,
        )
        my_store_backend.remove_key(
            (
                "rendered_data_doc",
                "1ccac18e-7631-4bdd-8a42-3c35cce574c6",
            )
        )
        mock_delete.assert_called_with(
            "https://app.greatexpectations.io/organizations/51379b8b-86d3-4fe7-84e9-e1a52f4a414c/rendered-data-docs/1ccac18e-7631"
            "-4bdd"
            "-8a42-3c35cce574c6",
            json={
                "data": {
                    "type": "rendered_data_doc",
                    "id_": "1ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    "attributes": {"deleted": True},
                }
            },
            **shared_called_with_request_kwargs,
        )


@pytest.mark.unit
def test_GeCloudStoreBackend_casts_str_resource_type_to_GeCloudRESTResource() -> None:
    ge_cloud_base_url = "https://app.greatexpectations.io/"
    ge_cloud_credentials = {
        "access_token": "1234",
        "organization_id": "51379b8b-86d3-4fe7-84e9-e1a52f4a414c",
    }
    ge_cloud_resource_type = "checkpoint"  # Instead of using enum

    my_store_backend = GeCloudStoreBackend(
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_credentials=ge_cloud_credentials,
        ge_cloud_resource_type=ge_cloud_resource_type,
    )

    assert my_store_backend.ge_cloud_resource_type is GeCloudRESTResource.CHECKPOINT
