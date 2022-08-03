"""This file is meant for integration tests related to datasource CRUD."""
import copy
from typing import Callable
from unittest.mock import patch

import pytest

from great_expectations import DataContext
from great_expectations.data_context import BaseDataContext, CloudDataContext
from great_expectations.data_context.types.base import DatasourceConfig
from great_expectations.datasource import BaseDatasource
from tests.data_context.cloud_data_context.conftest import MockResponse


@pytest.fixture
def datasource_id() -> str:
    return "some_uuid"


@pytest.fixture
def mocked_get_response(
    mock_response_factory: Callable,
    datasource_name: str,
    datasource_id: str,
) -> Callable[[], MockResponse]:
    def _mocked_get_response(*args, **kwargs):
        datasource_config_with_ids: dict = {
            "name": datasource_name,
            "data_connectors": {
                "tripdata_monthly_configured": {
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "base_directory": "/path/to/trip_data",
                    "assets": {
                        "yellow": {
                            "group_names": ["year", "month"],
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "pattern": "yellow_tripdata_(\\d{4})-(\\d{2})\\.csv$",
                            "class_name": "Asset",
                        }
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                }
            },
            "module_name": "great_expectations.datasource",
            "class_name": "Datasource",
            "id": datasource_id,
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
        }

        # TODO: AJB 20220803 Add the rest of the mocked response
        return mock_response_factory(
            {
                "data": {
                    "attributes": {"datasource_config": datasource_config_with_ids},
                    "id": datasource_id,
                }
            },
            200,
        )

    return _mocked_get_response


@pytest.mark.cloud
@pytest.mark.e2e
@pytest.mark.parametrize(
    "save_changes",
    [
        pytest.param(True, id="save_changes=True"),
        # pytest.param(False, id="save_changes=False"),
    ],
)
@pytest.mark.parametrize(
    "config_includes_name_setting",
    [
        pytest.param("name_supplied_separately", id="name supplied separately"),
        # pytest.param("config_includes_name", id="config includes name"),
        # pytest.param(
        #     "name_supplied_separately_and_included_in_config",
        #     id="name supplied separately and config includes name",
        #     marks=pytest.mark.xfail(strict=True, raises=TypeError),
        # ),
    ],
)
def test_base_data_context_in_cloud_mode_add_datasource(
    save_changes: bool,
    config_includes_name_setting: str,
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    datasource_config: DatasourceConfig,
    datasource_name: str,
    datasource_id: str,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    request_headers: dict,
    mock_response_factory: Callable,
    mocked_get_response: Callable[[], MockResponse],
):
    """A BaseDataContext in cloud mode should save to the cloud backed Datasource store when calling add_datasource
    with save_changes=True and not save when save_changes=False. When saving, it should use the id from the response
    to create the datasource."""

    context: BaseDataContext = empty_base_data_context_in_cloud_mode
    # Make sure the fixture has the right configuration
    assert isinstance(context, BaseDataContext)
    assert context.ge_cloud_mode
    assert len(context.list_datasources()) == 0

    # Setup
    datasource_config_with_name: DatasourceConfig = copy.deepcopy(datasource_config)
    datasource_config_with_name.name = datasource_name

    def mocked_post_response(*args, **kwargs):
        return mock_response_factory({"data": {"id": datasource_id}}, 201)

    with patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post, patch(
        "requests.get", autospec=True, side_effect=mocked_get_response
    ) as mock_get:

        # Call add_datasource with and without the name field included in the datasource config
        stored_datasource: BaseDatasource
        if config_includes_name_setting == "name_supplied_separately":
            stored_datasource = context.add_datasource(
                name=datasource_name,
                **datasource_config.to_dict(),
                save_changes=save_changes,
            )
        elif config_includes_name_setting == "config_includes_name":
            stored_datasource = context.add_datasource(
                **datasource_config_with_name.to_dict(), save_changes=save_changes
            )
        elif (
            config_includes_name_setting
            == "name_supplied_separately_and_included_in_config"
        ):
            stored_datasource = context.add_datasource(
                name=datasource_name,
                **datasource_config_with_name.to_dict(),
                save_changes=save_changes,
            )

        # Make sure we have stored our datasource in the context
        assert len(context.list_datasources()) == 1

        retrieved_datasource: BaseDatasource = context.get_datasource(datasource_name)

        # This post should have been called without the id (which is retrieved from the response).
        # It should have been called with the datasource name in the config.
        if save_changes:
            mock_post.assert_called_with(
                f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources",
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
            mock_get.assert_called_once()
            # TODO: AJB 20220803 Assert that mock_get is called also, make sure that the same id is used in both calls
        else:
            assert not mock_post.called
            assert not mock_get.called

        if save_changes:
            # Make sure the id was populated correctly into the created datasource object and config
            assert stored_datasource.id_ == datasource_id
            assert retrieved_datasource.id_ == datasource_id
            assert retrieved_datasource.config["id_"] == datasource_id
        else:
            assert stored_datasource.id_ is None
            assert retrieved_datasource.id_ is None
            assert retrieved_datasource.config["id_"] is None

        # Make sure the name is populated correctly into the created datasource
        assert retrieved_datasource.name == datasource_name
        assert retrieved_datasource.config["name"] == datasource_name

# commented out on AB branch
@pytest.mark.cloud
@pytest.mark.e2e
@pytest.mark.parametrize(
    "config_includes_name_setting",
    [
        pytest.param("name_supplied_separately", id="name supplied separately"),
        pytest.param("config_includes_name", id="config includes name"),
        pytest.param(
            "name_supplied_separately_and_included_in_config",
            id="name supplied separately and config includes name",
            marks=pytest.mark.xfail(strict=True, raises=TypeError),
        ),
    ],
)
def test_data_context_in_cloud_mode_add_datasource(
    config_includes_name_setting: str,
    empty_data_context_in_cloud_mode: DataContext,
    datasource_config: DatasourceConfig,
    datasource_name: str,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    request_headers: dict,
    mock_response_factory: Callable,
):
    """A DataContext in cloud mode should save to the cloud backed Datasource store when calling add_datasource. When saving, it should use the id from the response
    to create the datasource."""

    context: DataContext = empty_data_context_in_cloud_mode
    # Make sure the fixture has the right configuration
    assert isinstance(context, DataContext)
    assert context.ge_cloud_mode
    assert len(context.list_datasources()) == 0

    # Setup
    datasource_id: str = "some_uuid"
    datasource_config_with_name: DatasourceConfig = copy.deepcopy(datasource_config)
    datasource_config_with_name.name = datasource_name

    def mocked_post_response(*args, **kwargs):
        return mock_response_factory({"data": {"id": datasource_id}}, 201)

    with patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post:

        # Call add_datasource with and without the name field included in the datasource config
        stored_datasource: BaseDatasource
        if config_includes_name_setting == "name_supplied_separately":
            stored_datasource = context.add_datasource(
                name=datasource_name,
                **datasource_config.to_dict(),
            )
        elif config_includes_name_setting == "config_includes_name":
            stored_datasource = context.add_datasource(
                **datasource_config_with_name.to_dict()
            )
        elif (
            config_includes_name_setting
            == "name_supplied_separately_and_included_in_config"
        ):
            stored_datasource = context.add_datasource(
                name=datasource_name,
                **datasource_config_with_name.to_dict(),
            )

        # Make sure we have stored our datasource in the context
        assert len(context.list_datasources()) == 1

        retrieved_datasource: BaseDatasource = context.get_datasource(datasource_name)

        # This post should have been called without the id (which is retrieved from the response).
        # It should have been called with the datasource name in the config.
        mock_post.assert_called_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources",
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

        # Make sure the id was populated correctly into the created datasource object and config
        assert stored_datasource.id_ == datasource_id
        assert retrieved_datasource.id_ == datasource_id
        assert retrieved_datasource.config["id_"] == datasource_id

        # Make sure the name is populated correctly into the created datasource
        assert retrieved_datasource.name == datasource_name
        assert retrieved_datasource.config["name"] == datasource_name


@pytest.mark.cloud
@pytest.mark.e2e
@pytest.mark.parametrize(
    "config_includes_name_setting",
    [
        pytest.param("name_supplied_separately", id="name supplied separately"),
        pytest.param("config_includes_name", id="config includes name"),
        pytest.param(
            "name_supplied_separately_and_included_in_config",
            id="name supplied separately and config includes name",
            marks=pytest.mark.xfail(strict=True, raises=TypeError),
        ),
    ],
)
def test_cloud_data_context_add_datasource(
    config_includes_name_setting: str,
    empty_cloud_data_context: CloudDataContext,
    datasource_config: DatasourceConfig,
    datasource_name: str,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    request_headers: dict,
    mock_response_factory: Callable,
):
    """A CloudDataContext should save to the cloud backed Datasource store when calling add_datasource. When saving, it should use the id from the response
    to create the datasource."""

    context: CloudDataContext = empty_cloud_data_context
    # Make sure the fixture has the right configuration
    assert isinstance(context, CloudDataContext)
    assert context.ge_cloud_mode
    assert len(context.list_datasources()) == 0

    # Setup
    datasource_id: str = "some_uuid"
    datasource_config_with_name: DatasourceConfig = copy.deepcopy(datasource_config)
    datasource_config_with_name.name = datasource_name

    def mocked_post_response(*args, **kwargs):
        return mock_response_factory({"data": {"id": datasource_id}}, 201)

    with patch(
        "requests.post", autospec=True, side_effect=mocked_post_response
    ) as mock_post:

        # Call add_datasource with and without the name field included in the datasource config
        stored_datasource: BaseDatasource
        if config_includes_name_setting == "name_supplied_separately":
            stored_datasource = context.add_datasource(
                name=datasource_name,
                **datasource_config.to_dict(),
                save_changes=True,
            )
        elif config_includes_name_setting == "config_includes_name":
            stored_datasource = context.add_datasource(
                **datasource_config_with_name.to_dict(),
                save_changes=True,
            )
        elif (
            config_includes_name_setting
            == "name_supplied_separately_and_included_in_config"
        ):
            stored_datasource = context.add_datasource(
                name=datasource_name,
                **datasource_config_with_name.to_dict(),
                save_changes=True,
            )

        # Make sure we have stored our datasource in the context
        assert len(context.list_datasources()) == 1

        retrieved_datasource: BaseDatasource = context.get_datasource(datasource_name)

        # This post should have been called without the id (which is retrieved from the response).
        # It should have been called with the datasource name in the config.
        mock_post.assert_called_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/datasources",
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

        # Make sure the id was populated correctly into the created datasource object and config
        assert stored_datasource.id_ == datasource_id
        assert retrieved_datasource.id_ == datasource_id
        assert retrieved_datasource.config["id_"] == datasource_id

        # Make sure the name is populated correctly into the created datasource
        assert retrieved_datasource.name == datasource_name
        assert retrieved_datasource.config["name"] == datasource_name
