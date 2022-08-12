from unittest.mock import PropertyMock, patch

import pytest

from great_expectations.data_context.store import DatasourceStore
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier

from ..cloud_data_context.conftest import MockResponse


@pytest.mark.cloud
@pytest.mark.unit
def test_datasource_store_create(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
    datasource_config: DatasourceConfig,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when creating a new datasource.
    """

    # Note: id will be provided by the backend on create
    key = GeCloudIdentifier(
        resource_type=GeCloudRESTResource.DATASOURCE,
    )

    with patch("requests.post", autospec=True) as mock_post:
        type(mock_post.return_value).status_code = PropertyMock(return_value=200)

        datasource_store_ge_cloud_backend.set(key=key, value=datasource_config)

        expected_datasource_config = datasourceConfigSchema.dump(datasource_config)

        mock_post.assert_called_once_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources",
            json={
                "data": {
                    "type": "datasource",
                    "attributes": {
                        "datasource_config": expected_datasource_config,
                        "organization_id": ge_cloud_organization_id,
                    },
                }
            },
            **shared_called_with_request_kwargs,
        )


@pytest.mark.cloud
@pytest.mark.unit
def test_datasource_store_get_by_id(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
    datasource_config: DatasourceConfig,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when getting a datasource.
    """

    id_: str = "example_id_normally_uuid"

    key = GeCloudIdentifier(
        resource_type=GeCloudRESTResource.DATASOURCE, ge_cloud_id=id_
    )

    def mocked_response(*args, **kwargs):

        return MockResponse(
            {
                "data": {
                    "id_": id_,
                    "attributes": {"datasource_config": datasource_config},
                }
            },
            200,
        )

    with patch("requests.get", autospec=True, side_effect=mocked_response) as mock_get:

        datasource_store_ge_cloud_backend.get(key=key)

        mock_get.assert_called_once_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources/{id_}",
            params=None,
            **shared_called_with_request_kwargs,
        )


@pytest.mark.cloud
@pytest.mark.unit
def test_datasource_store_get_by_name(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
    datasource_config: DatasourceConfig,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when getting a datasource with a name.
    """

    id_: str = "example_id_normally_uuid"
    datasource_name: str = "example_datasource_config_name"

    def mocked_response(*args, **kwargs):

        return MockResponse(
            {
                "data": {
                    "id_": id_,
                    "attributes": {"datasource_config": datasource_config},
                }
            },
            200,
        )

    with patch(
        "requests.get", autospec=True, side_effect=mocked_response
    ) as mock_get, patch(
        "great_expectations.data_context.store.DatasourceStore.has_key", autospec=True
    ) as mock_has_key:
        # Mocking has_key so that we don't try to connect to the cloud backend to verify key existence.
        mock_has_key.return_value = True

        datasource_store_ge_cloud_backend.retrieve_by_name(
            datasource_name=datasource_name
        )

        mock_get.assert_called_once_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources",
            params={"name": datasource_name},
            **shared_called_with_request_kwargs,
        )


@pytest.mark.cloud
@pytest.mark.unit
def test_datasource_store_delete_by_id(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
    datasource_config: DatasourceConfig,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when getting a datasource.
    """
    id_: str = "example_id_normally_uuid"

    key = GeCloudIdentifier(
        resource_type=GeCloudRESTResource.DATASOURCE, ge_cloud_id=id_
    )

    with patch("requests.delete", autospec=True) as mock_delete:
        type(mock_delete.return_value).status_code = PropertyMock(return_value=200)

        datasource_store_ge_cloud_backend.remove_key(key=key)

        mock_delete.assert_called_once_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources/{id_}",
            json={
                "data": {
                    "type": "datasource",
                    "id_": id_,
                    "attributes": {"deleted": True},
                }
            },
            **shared_called_with_request_kwargs,
        )
