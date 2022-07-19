from unittest.mock import PropertyMock, patch

from great_expectations.data_context.store import DatasourceStore
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import DatasourceConfig
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier


def test_datasource_store_create(
    ge_cloud_base_url: str,
    ge_cloud_access_token: str,
    ge_cloud_organization_id: str,
    datasource_config: DatasourceConfig,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request.
    """

    # Note: id will be provided by the backend on create
    key: GeCloudIdentifier = GeCloudIdentifier(
        resource_type=GeCloudRESTResource.DATASOURCE.value,
    )

    with patch("requests.post", autospec=True) as mock_post:
        type(mock_post.return_value).status_code = PropertyMock(return_value=200)

        datasource_store_ge_cloud_backend.set(key=key, value=datasource_config)

        mock_post.assert_called_with(
            f"{ge_cloud_base_url}/organizations/bd20fead-2c31-4392-bcd1-f1e87ad5a79c/datasources",
            json={
                "data": {
                    "type": "datasource",
                    "attributes": {
                        "organization_id": ge_cloud_organization_id,
                        "datasource_config": datasource_config.to_dict(),
                    },
                }
            },
            headers={
                "Content-Type": "application/vnd.api+json",
                "Authorization": f"Bearer {ge_cloud_access_token}",
            },
        )


def test_datasource_store_get_by_id():
    raise NotImplementedError


def test_datasource_store_update_by_id():
    raise NotImplementedError


def test_datasource_store_delete_by_id():
    raise NotImplementedError
