"""This file is meant for integration tests related to datasource CRUD."""
import copy
import enum
from typing import Callable, Type, Union
from unittest.mock import patch

import pytest

from great_expectations import DataContext
from great_expectations.data_context import (
    AbstractDataContext,
    BaseDataContext,
    CloudDataContext,
)
from great_expectations.data_context.store import GeCloudStoreBackend
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
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


class ConfigType(enum.Enum):
    NAME_SUPPLIED_SEPARATELY: str = "name_supplied_separately"
    CONFIG_INCLUDES_NAME: str = "config_includes_name"
    NAME_SUPPLIED_SEPARATELY_AND_INCLUDED_IN_CONFIG: str = (
        "name_supplied_separately_and_included_in_config"
    )


def _add_datasource_helper(
    context: AbstractDataContext,
    config_type: ConfigType,
    save_changes: bool,
    datasource_name: str,
    datasource_config: DatasourceConfig,
) -> BaseDatasource:
    """Helper method to call add_datasource with the right parameters based on the test parametrization config."""
    datasource_config_with_name: DatasourceConfig = copy.deepcopy(datasource_config)
    datasource_config_with_name.name = datasource_name

    # Call add_datasource with and without the name field included in the datasource config
    stored_datasource: BaseDatasource
    if isinstance(context, DataContext):
        # Don't add the `save_changes` param for DataContext
        name_and_save_changes_params_base: dict = {}
    else:
        name_and_save_changes_params_base: dict = {"save_changes": save_changes}

    if config_type == ConfigType.NAME_SUPPLIED_SEPARATELY:
        name_and_save_changes_params: dict = name_and_save_changes_params_base.copy()
        name_and_save_changes_params.update({"name": datasource_name})
        stored_datasource = context.add_datasource(
            **name_and_save_changes_params,
            **datasource_config.to_dict(),
        )
    elif config_type == ConfigType.CONFIG_INCLUDES_NAME:
        name_and_save_changes_params: dict = name_and_save_changes_params_base.copy()
        name_and_save_changes_params.update({})
        stored_datasource = context.add_datasource(
            **name_and_save_changes_params,
            **datasource_config_with_name.to_dict(),
        )
    elif config_type == ConfigType.NAME_SUPPLIED_SEPARATELY_AND_INCLUDED_IN_CONFIG:
        name_and_save_changes_params: dict = name_and_save_changes_params_base.copy()
        name_and_save_changes_params.update({"name": datasource_name})
        stored_datasource = context.add_datasource(
            **name_and_save_changes_params,
            **datasource_config_with_name.to_dict(),
        )
    else:
        raise Exception(
            f"config_type must be one of {','.join([str(config_type) for config_type in ConfigType])}"
        )
    return stored_datasource


@pytest.mark.cloud
@pytest.mark.integration
@pytest.mark.parametrize(
    "data_context_fixture_name,data_context_type,save_changes",
    [
        # In order to leverage existing fixtures in parametrization, we provide
        # their string names and dynamically retrieve them using pytest's built-in
        # `request` fixture.
        # Source: https://stackoverflow.com/a/64348247
        pytest.param(
            "empty_base_data_context_in_cloud_mode",
            BaseDataContext,
            True,
            id="BaseDataContext save_changes=True",
        ),
        pytest.param(
            "empty_base_data_context_in_cloud_mode",
            BaseDataContext,
            False,
            id="BaseDataContext save_changes=False",
        ),
        # Note DataContext defaults save_changes to True so there is no save_changes=False test case.
        pytest.param(
            "empty_data_context_in_cloud_mode",
            DataContext,
            True,
            id="DataContext save_changes defaulted to True",
        ),
        pytest.param(
            "empty_cloud_data_context",
            CloudDataContext,
            True,
            id="CloudDataContext save_changes=True",
        ),
        pytest.param(
            "empty_cloud_data_context",
            CloudDataContext,
            False,
            id="CloudDataContext save_changes=False",
        ),
    ],
)
@pytest.mark.parametrize(
    "config_type",
    [
        pytest.param(
            ConfigType.NAME_SUPPLIED_SEPARATELY, id="name supplied separately"
        ),
        pytest.param(ConfigType.CONFIG_INCLUDES_NAME, id="config includes name"),
        pytest.param(
            ConfigType.NAME_SUPPLIED_SEPARATELY_AND_INCLUDED_IN_CONFIG,
            id="name supplied separately and config includes name",
            marks=pytest.mark.xfail(strict=True, raises=TypeError),
        ),
    ],
)
def test_cloud_backed_data_context_add_datasource(
    data_context_fixture_name: str,
    data_context_type: Type[AbstractDataContext],
    save_changes: bool,
    config_type: ConfigType,
    datasource_config: DatasourceConfig,
    datasource_name: str,
    datasource_id: str,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
    mock_response_factory: Callable,
    mocked_get_response: Callable[[], MockResponse],
    request,
) -> None:

    """test_add_datasource_in_all_data_contexts_supporting_cloud_mode Any Data Context in cloud mode should save to the cloud backed Datasource store when calling add_datasource with save_changes=True and not save when save_changes=False. When saving, it should use the id from the response to create the datasource."""

    context: Union[
        BaseDataContext, DataContext, CloudDataContext
    ] = request.getfixturevalue(data_context_fixture_name)

    # Make sure the fixture has the right configuration
    assert isinstance(context, data_context_type)
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

        stored_datasource: BaseDatasource = _add_datasource_helper(
            context=context,
            config_type=config_type,
            save_changes=save_changes,
            datasource_name=datasource_name,
            datasource_config=datasource_config,
        )

        # Make sure we have stored our datasource in the context
        assert len(context.list_datasources()) == 1

        retrieved_datasource: BaseDatasource = context.get_datasource(datasource_name)

        # Round trip through schema to mimic updates made during store serialization process
        expected_datasource_config = datasourceConfigSchema.dump(
            DatasourceConfig(**datasource_config_with_name.to_json_dict())
        )

        # This post should have been called without the id (which is retrieved from the response).
        # It should have been called with the datasource name in the config.
        if save_changes:
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
    datasource_name: str,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
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

        # Make sure we have stored our datasource in the context
        assert len(context.list_datasources()) == 1

        retrieved_datasource: BaseDatasource = context.get_datasource(datasource_name)
        expected_datasource_config = datasourceConfigSchema.dump(
            datasource_config_with_name
        )

        # This post should have been called without the id (which is retrieved from the response).
        # It should have been called with the datasource name in the config.
        mock_post.assert_called_with(
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

        # Make sure the id was populated correctly into the created datasource object and config
        assert stored_datasource.id_ == datasource_id
        assert retrieved_datasource.id_ == datasource_id
        assert retrieved_datasource.config["id_"] == datasource_id

        # Make sure the name is populated correctly into the created datasource
        assert retrieved_datasource.name == datasource_name
        assert retrieved_datasource.config["name"] == datasource_name


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
    datasource_name: str,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
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

        # Make sure we have stored our datasource in the context
        assert len(context.list_datasources()) == 1

        retrieved_datasource: BaseDatasource = context.get_datasource(datasource_name)
        expected_datasource_config = datasourceConfigSchema.dump(
            datasource_config_with_name
        )

        # This post should have been called without the id (which is retrieved from the response).
        # It should have been called with the datasource name in the config.
        mock_post.assert_called_with(
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

        # Make sure the id was populated correctly into the created datasource object and config
        assert stored_datasource.id_ == datasource_id
        assert retrieved_datasource.id_ == datasource_id
        assert retrieved_datasource.config["id_"] == datasource_id

        # Make sure the name is populated correctly into the created datasource
        assert retrieved_datasource.name == datasource_name
        assert retrieved_datasource.config["name"] == datasource_name
