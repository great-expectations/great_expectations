"""This file is meant for integration tests related to datasource CRUD."""
import copy
from unittest.mock import patch

import pytest

# TODO: Tests
# Test add_datasource
# Test get_datasource
# Test update_datasource
# Test delete_datasource
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.store import GeCloudStoreBackend
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)

# TODO: Fixtures
# empty_cloud_data_context for a BaseDataContext in cloud mode
# ?? new fixture for a CloudDataContext data context
# ?? for DataContext in cloud mode
from great_expectations.data_context.types.refs import GeCloudResourceRef
from great_expectations.datasource import BaseDatasource


@pytest.mark.integration
def test_cloud_mode_data_context_add_datasource_save_changes_true():
    """A BaseDataContext in cloud mode should save to the Datasource store when calling add_datasource
    with save_changes=True."""
    # 1. setup context
    # 2. call add_datasource with mocked `requests.set`
    # 3. assert the right call was made to the store backend
    raise NotImplementedError


@pytest.mark.integration
def test_cloud_mode_data_context_add_datasource_save_changes_false():
    """A BaseDataContext in cloud mode should not save to the Datasource store when calling add_datasource
    with save_changes=False."""

    # Assert no call was made to the store backend
    raise NotImplementedError


@pytest.mark.integration
def test_cloud_mode_base_data_context_add_datasource_save_changes_true(
    empty_cloud_data_context: BaseDataContext,
    datasource_config: DatasourceConfig,
    datasource_name: str,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    request_headers: dict,
):
    """A BaseDataContext in cloud mode should save to the Datasource store when calling add_datasource
    with save_changes=True."""
    assert empty_cloud_data_context.ge_cloud_mode
    assert isinstance(
        empty_cloud_data_context._data_context._datasource_store._store_backend,
        GeCloudStoreBackend,
    )

    datasource_id: str = "some_uuid"
    datasource_config_with_name: DatasourceConfig = copy.deepcopy(datasource_config)
    datasource_config_with_name.name = datasource_name

    def mocked_post_response(*args, **kwargs):
        class MockResponse:
            def __init__(self, json_data: dict, status_code: int) -> None:
                self._json_data = json_data
                self._status_code = status_code

            def json(self):
                return self._json_data

        return MockResponse({"data": {"id": datasource_id}}, 201)

    with patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post:
        datasource: BaseDatasource = empty_cloud_data_context.add_datasource(
            name=datasource_name, **datasource_config.to_dict(), save_changes=True
        )
        assert len(empty_cloud_data_context.list_datasources()) == 1
        # TODO: AJB 20220719 get this working and test elsewhere
        # assert empty_cloud_data_context.get_datasource(datasource_name).config == datasource_config.to_dict()

        stored_datasource: BaseDatasource = empty_cloud_data_context.get_datasource(
            datasource_name
        )
        # stored_datasource_config: DatasourceConfig = datasourceConfigSchema.load(**empty_cloud_data_context.list_datasources()[0])
        # TODO: AJB 20220725 - do we need config_with_name?
        stored_datasource_config: DatasourceConfig = datasourceConfigSchema.load(
            stored_datasource.config_with_name
        )

        # TODO: AJB 20220725 REMOVE THIS HACK
        ge_cloud_base_url = (
            empty_cloud_data_context._datasource_store._store_backend.ge_cloud_base_url
        )

        # This post should be called without the id (which is retrieved from the response).
        # It should be called with the datasource name in the config.
        mock_post.assert_called_with(
            f"{ge_cloud_base_url}organizations/{ge_cloud_organization_id}/datasources",
            json={
                "data": {
                    "type": "datasource",
                    "attributes": {
                        "datasource_config": datasource_config_with_name.to_json_dict(),
                        "organization_id": ge_cloud_organization_id,
                    },
                }
            },
            headers=request_headers,
        )

        # type(mock_post.return_value).status_code = PropertyMock(return_value=200)
        #
        # datasource_store_ge_cloud_backend.set(key=key, value=datasource_config)
        #

        assert datasource.id_ == datasource_id


@pytest.mark.integration
def test_cloud_mode_base_data_context_add_datasource_save_changes_false():
    """A BaseDataContext in cloud mode should not save to the Datasource store when calling add_datasource
    with save_changes=False."""
    raise NotImplementedError


@pytest.mark.integration
def test_cloud_data_context_add_datasources_save_changes_true():
    raise NotImplementedError


@pytest.mark.integration
def test_cloud_data_context_add_datasources_save_changes_false():
    raise NotImplementedError
