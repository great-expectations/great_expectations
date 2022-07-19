from unittest.mock import PropertyMock, patch

from great_expectations.data_context.store import DatasourceStore
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import DatasourceConfig
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier


def test_datasource_store_create(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    request_headers: dict,
    datasource_config: DatasourceConfig,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when creating a new datasource.
    """

    # Note: id will be provided by the backend on create
    key: GeCloudIdentifier = GeCloudIdentifier(
        resource_type=GeCloudRESTResource.DATASOURCE,
    )

    with patch("requests.post", autospec=True) as mock_post:
        type(mock_post.return_value).status_code = PropertyMock(return_value=200)

        datasource_store_ge_cloud_backend.set(key=key, value=datasource_config)

        mock_post.assert_called_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources",
            json={
                "data": {
                    "type": "datasource",
                    "attributes": {
                        "datasource_config": datasource_config.to_dict(),
                        "organization_id": ge_cloud_organization_id,
                    },
                }
            },
            headers=request_headers,
        )


def test_datasource_store_get_by_id(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    request_headers: dict,
    datasource_config: DatasourceConfig,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when getting a datasource.
    """

    id_: str = "example_id_normally_uuid"

    key: GeCloudIdentifier = GeCloudIdentifier(
        resource_type=GeCloudRESTResource.DATASOURCE, ge_cloud_id=id_
    )

    with patch("requests.get", autospec=True) as mock_get:
        # type(mock_get.return_value).status_code = PropertyMock(return_value=200)
        # type(mock_get.return_value).json = PropertyMock(return_value={})

        datasource_store_ge_cloud_backend.get(key=key)

        mock_get.assert_called_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources/{id_}",
            headers=request_headers,
        )


def test_datasource_store_delete_by_id(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    request_headers: dict,
    datasource_config: DatasourceConfig,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when getting a datasource.
    """
    id_: str = "example_id_normally_uuid"

    key: GeCloudIdentifier = GeCloudIdentifier(
        resource_type=GeCloudRESTResource.DATASOURCE, ge_cloud_id=id_
    )

    with patch("requests.delete", autospec=True) as mock_delete:
        type(mock_delete.return_value).status_code = PropertyMock(return_value=200)

        datasource_store_ge_cloud_backend.remove_key(key=key)

        mock_delete.assert_called_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources/{id_}",
            json={
                "data": {
                    "type": "datasource",
                    "id": id_,
                    "attributes": {"deleted": True},
                }
            },
            headers=request_headers,
        )
