import urllib.parse
from typing import Dict
from unittest import mock

import pytest

from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import DatasourceStore
from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource
from great_expectations.exceptions import StoreBackendError
from tests.data_context.conftest import MockResponse


@pytest.mark.cloud
def test_datasource_store_get_by_id(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when getting a datasource.
    """  # noqa: E501

    id = "8706d5fb-0432-47ab-943c-daa824210e99"

    key = GXCloudIdentifier(resource_type=GXCloudRESTResource.DATASOURCE, id=id)

    def mocked_response(*args, **kwargs):
        return MockResponse(
            {
                "data": {
                    "id": id,
                    "name": "my_datasource",
                    "type": "pandas",
                }
            },
            200,
        )

    with mock.patch("requests.Session.get", autospec=True, side_effect=mocked_response) as mock_get:
        datasource_store_ge_cloud_backend.get(key=key)

        mock_get.assert_called_once_with(
            mock.ANY,  # requests.Session object
            urllib.parse.urljoin(
                ge_cloud_base_url,
                f"api/v1/organizations/{ge_cloud_organization_id}/datasources/{id}",
            ),
            params=None,
        )


@pytest.mark.cloud
def test_datasource_store_get_by_name(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when getting a datasource with a name.
    """  # noqa: E501

    id = "8706d5fb-0432-47ab-943c-daa824210e99"
    datasource_name: str = "example_datasource_config_name"

    def mocked_response(*args, **kwargs):
        return MockResponse(
            {
                "data": {
                    "id": id,
                    "name": "my_datasource",
                    "type": "pandas",
                }
            },
            200,
        )

    with (
        mock.patch("requests.Session.get", autospec=True, side_effect=mocked_response) as mock_get,
        mock.patch(
            "great_expectations.data_context.store.DatasourceStore.has_key", autospec=True
        ) as mock_has_key,
    ):
        # Mocking has_key so that we don't try to connect to the cloud backend to verify key existence.  # noqa: E501
        mock_has_key.return_value = True

        datasource_store_ge_cloud_backend.retrieve_by_name(name=datasource_name)

        mock_get.assert_called_once_with(
            mock.ANY,  # requests.Session object
            urllib.parse.urljoin(
                ge_cloud_base_url, f"api/v1/organizations/{ge_cloud_organization_id}/datasources"
            ),
            params={"name": datasource_name},
        )


@pytest.mark.cloud
def test_datasource_store_delete_by_id(
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    datasource_store_ge_cloud_backend: DatasourceStore,
) -> None:
    """What does this test and why?

    The datasource store when used with a cloud backend should emit the correct request when getting a datasource.
    """  # noqa: E501
    id: str = "example_id_normally_uuid"

    key = GXCloudIdentifier(resource_type=GXCloudRESTResource.DATASOURCE, id=id)

    with mock.patch("requests.Session.delete", autospec=True) as mock_delete:
        type(mock_delete.return_value).status_code = mock.PropertyMock(return_value=200)

        datasource_store_ge_cloud_backend.remove_key(key=key)

        mock_delete.assert_called_once_with(
            mock.ANY,  # requests.Session object
            urllib.parse.urljoin(
                ge_cloud_base_url,
                f"api/v1/organizations/{ge_cloud_organization_id}/datasources/{id}",
            ),
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "http_verb,method,args",
    [
        ("get", "get", []),
        ("put", "set", [PandasDatasource(name="my_datasource")]),
        pytest.param(
            "delete",
            "delete",
            [],
            marks=pytest.mark.xfail(reason="We do not raise errors on delete fail", strict=True),
        ),
    ],
)
def test_datasource_http_error_handling(
    datasource_store_ge_cloud_backend: DatasourceStore,
    mock_http_unavailable: Dict[str, mock.Mock],  # noqa: TID251
    http_verb: str,
    method: str,
    args: list,
):
    id: str = "example_id_normally_uuid"

    key = GXCloudIdentifier(resource_type=GXCloudRESTResource.DATASOURCE, id=id)
    with pytest.raises(
        StoreBackendError, match=r"Unable to \w+ object in GX Cloud Store Backend: .*"
    ) as exc_info:
        backend_method = getattr(datasource_store_ge_cloud_backend, method)
        backend_method(key, *args)

    print(f"Exception details:\n\t{exc_info.type}\n\t{exc_info.value}")

    mock_http_unavailable[http_verb].assert_called_once()
