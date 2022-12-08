"""This file is meant for integration tests related to datasource CRUD."""
import copy
import random
import string
from typing import Callable, cast
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations import DataContext
from great_expectations.data_context import BaseDataContext, CloudDataContext
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource import BaseDatasource
from great_expectations.datasource.datasource_serializer import (
    JsonDatasourceConfigSerializer,
)
from great_expectations.datasource.new_datasource import Datasource
from tests.data_context.conftest import MockResponse


@pytest.mark.cloud
@pytest.mark.integration
@pytest.mark.parametrize(
    "save_changes",
    [
        pytest.param(True, id="save_changes=True"),
        pytest.param(False, id="save_changes=False"),
    ],
)
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
def test_base_data_context_in_cloud_mode_add_datasource(
    save_changes: bool,
    config_includes_name_setting: str,
    empty_base_data_context_in_cloud_mode: BaseDataContext,
    datasource_config: DatasourceConfig,
    datasource_config_with_names_and_ids: DatasourceConfig,
    fake_datasource_id: str,
    fake_data_connector_id: str,
    mocked_datasource_post_response: Callable[[], MockResponse],
    mocked_datasource_get_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
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
    datasource_name = datasource_config_with_names_and_ids.name
    datasource_config_with_name: DatasourceConfig = copy.deepcopy(datasource_config)
    datasource_config_with_name.name = datasource_name

    with mock.patch(
        "requests.Session.post",
        autospec=True,
        side_effect=mocked_datasource_post_response,
    ) as mock_post, mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_datasource_get_response,
    ):

        # Call add_datasource with and without the name field included in the datasource config
        stored_datasource: BaseDatasource
        if config_includes_name_setting == "name_supplied_separately":
            expected_datasource_config = datasourceConfigSchema.dump(datasource_config)
            stored_datasource = context.add_datasource(
                name=datasource_name,
                **expected_datasource_config,
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
        else:
            raise ValueError(
                "Invalid value provided for 'config_includes_name_setting'"
            )

        # Make sure we have stored our datasource in the context
        assert len(context.list_datasources()) == 1

        serializer = JsonDatasourceConfigSerializer(schema=datasourceConfigSchema)
        expected_datasource_config = serializer.serialize(datasource_config_with_name)

        # This post should have been called without the id (which is retrieved from the response).
        # It should have been called with the datasource name in the config.
        if save_changes:
            mock_post.assert_called_with(
                mock.ANY,  # requests.Session object
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
            )
        else:
            assert not mock_post.called

        data_connector_name = tuple(stored_datasource.data_connectors.keys())[0]
        stored_data_connector = stored_datasource.data_connectors[data_connector_name]

        if save_changes:
            # Make sure the id was populated correctly into the created datasource object and config
            assert stored_datasource.id == fake_datasource_id
            assert stored_data_connector.id == fake_data_connector_id
        else:
            assert stored_datasource.id is None
            assert stored_data_connector.id is None

        # Make sure the name is populated correctly into the created datasource
        assert stored_datasource.name == datasource_name
        assert stored_datasource.config["name"] == datasource_name
        assert stored_data_connector.name == data_connector_name


@pytest.mark.cloud
@pytest.mark.integration
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
    datasource_config_with_names_and_ids: DatasourceConfig,
    fake_datasource_id: str,
    fake_data_connector_id: str,
    mocked_datasource_post_response: Callable[[], MockResponse],
    mocked_datasource_get_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
):
    """A DataContext in cloud mode should save to the cloud backed Datasource store when calling add_datasource. When saving, it should use the id from the response
    to create the datasource."""

    context: DataContext = empty_data_context_in_cloud_mode
    # Make sure the fixture has the right configuration
    assert isinstance(context, DataContext)
    assert context.ge_cloud_mode
    assert len(context.list_datasources()) == 0

    # Setup
    datasource_name = datasource_config_with_names_and_ids.name
    datasource_config_with_name: DatasourceConfig = copy.deepcopy(datasource_config)
    datasource_config_with_name.name = datasource_name

    with mock.patch(
        "requests.Session.post",
        autospec=True,
        side_effect=mocked_datasource_post_response,
    ) as mock_post, mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_datasource_get_response,
    ):

        # Call add_datasource with and without the name field included in the datasource config
        stored_datasource: BaseDatasource
        if config_includes_name_setting == "name_supplied_separately":
            expected_datasource_config = datasourceConfigSchema.dump(datasource_config)
            stored_datasource = context.add_datasource(
                name=datasource_name,
                **expected_datasource_config,
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
        else:
            raise ValueError(
                "Invalid value provided for 'config_includes_name_setting'"
            )

        # Make sure we have stored our datasource in the context
        assert len(context.list_datasources()) == 1

        serializer = JsonDatasourceConfigSerializer(schema=datasourceConfigSchema)
        expected_datasource_config = serializer.serialize(datasource_config_with_name)

        # This post should have been called without the id (which is retrieved from the response).
        # It should have been called with the datasource name in the config.
        mock_post.assert_called_with(
            mock.ANY,  # requests.Session object
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
        )

        data_connector_name = tuple(stored_datasource.data_connectors.keys())[0]
        stored_data_connector = stored_datasource.data_connectors[data_connector_name]

        # Make sure the id was populated correctly into the created datasource object and config
        assert stored_datasource.id == fake_datasource_id
        assert stored_data_connector.id == fake_data_connector_id

        # Make sure the name is populated correctly into the created datasource
        assert stored_datasource.name == datasource_name
        assert stored_datasource.config["name"] == datasource_name
        assert stored_data_connector.name == data_connector_name


@pytest.mark.cloud
@pytest.mark.integration
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
    datasource_config_with_names_and_ids: DatasourceConfig,
    fake_datasource_id: str,
    fake_data_connector_id: str,
    mocked_datasource_post_response: Callable[[], MockResponse],
    mocked_datasource_get_response: Callable[[], MockResponse],
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
):
    """A CloudDataContext should save to the cloud backed Datasource store when calling add_datasource. When saving, it should use the id from the response
    to create the datasource."""

    context: CloudDataContext = empty_cloud_data_context
    # Make sure the fixture has the right configuration
    assert isinstance(context, CloudDataContext)
    assert context.ge_cloud_mode
    assert len(context.list_datasources()) == 0

    # Setup
    datasource_name = datasource_config_with_names_and_ids.name
    datasource_config_with_name: DatasourceConfig = copy.deepcopy(datasource_config)
    datasource_config_with_name.name = datasource_name

    with mock.patch(
        "requests.Session.post",
        autospec=True,
        side_effect=mocked_datasource_post_response,
    ) as mock_post, mock.patch(
        "requests.Session.get",
        autospec=True,
        side_effect=mocked_datasource_get_response,
    ):

        # Call add_datasource with and without the name field included in the datasource config
        stored_datasource: BaseDatasource
        if config_includes_name_setting == "name_supplied_separately":
            expected_datasource_config = datasourceConfigSchema.dump(datasource_config)
            stored_datasource = context.add_datasource(
                name=datasource_name,
                **expected_datasource_config,
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
        else:
            raise ValueError(
                "Invalid value provided for 'config_includes_name_setting'"
            )

        # Make sure we have stored our datasource in the context
        assert len(context.list_datasources()) == 1

        serializer = JsonDatasourceConfigSerializer(schema=datasourceConfigSchema)
        expected_datasource_config = serializer.serialize(datasource_config_with_name)

        # This post should have been called without the id (which is retrieved from the response).
        # It should have been called with the datasource name in the config.
        mock_post.assert_called_with(
            mock.ANY,  # requests.Session object
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
        )

        data_connector_name = tuple(stored_datasource.data_connectors.keys())[0]
        stored_data_connector = stored_datasource.data_connectors[data_connector_name]

        # Make sure the id was populated correctly into the created datasource object and config
        assert stored_datasource.id == fake_datasource_id
        assert stored_data_connector.id == fake_data_connector_id

        # Make sure the name is populated correctly into the created datasource
        assert stored_datasource.name == datasource_name
        assert stored_datasource.config["name"] == datasource_name
        assert stored_data_connector.name == data_connector_name


@pytest.mark.e2e
@pytest.mark.cloud
def test_cloud_context_datasource_crud_e2e() -> None:
    context = cast(CloudDataContext, gx.get_context(cloud_mode=True))
    datasource_name = f"OSSTestDatasource_{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))}"
    datasource = Datasource(
        name=datasource_name,
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
        },
    )

    context.save_datasource(datasource)

    saved_datasource = context.get_datasource(datasource_name)
    assert saved_datasource is not None and saved_datasource.name == datasource_name

    context.delete_datasource(datasource_name)

    # Make another call to the backend to confirm deletion
    with pytest.raises(ValueError) as e:
        context.get_datasource(datasource_name)
    assert f"Unable to load datasource `{datasource_name}`" in str(e.value)
