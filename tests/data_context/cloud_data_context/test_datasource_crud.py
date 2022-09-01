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
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource import BaseDatasource
from great_expectations.datasource.datasource_serializer import (
    JsonDatasourceConfigSerializer,
)
from tests.data_context.conftest import MockResponse


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
        name_and_save_changes_params_base = {"save_changes": save_changes}

    name_and_save_changes_params: dict = name_and_save_changes_params_base.copy()
    if config_type == ConfigType.NAME_SUPPLIED_SEPARATELY:
        name_and_save_changes_params.update({"name": datasource_name})
        datasource_config_dict: dict = datasource_config.to_dict().copy()
        datasource_config_dict.pop(
            "name", None
        )  # make sure name (even if None) is not present
        stored_datasource = context.add_datasource(
            **name_and_save_changes_params,
            **datasource_config_dict,
        )
    elif config_type == ConfigType.CONFIG_INCLUDES_NAME:
        name_and_save_changes_params.update({})
        stored_datasource = context.add_datasource(
            **name_and_save_changes_params,
            **datasource_config_with_name.to_dict(),
        )
    elif config_type == ConfigType.NAME_SUPPLIED_SEPARATELY_AND_INCLUDED_IN_CONFIG:
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
    ) as mock_post:

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

        # Serialize expected config to mimic updates made during store serialization process
        serializer = JsonDatasourceConfigSerializer(schema=datasourceConfigSchema)
        expected_datasource_config = serializer.serialize(datasource_config_with_name)

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
        else:
            assert not mock_post.called

        if save_changes:
            # Make sure the id was populated correctly into the created datasource object and config
            assert stored_datasource.id == datasource_id
            assert retrieved_datasource.id == datasource_id
            assert retrieved_datasource.config["id"] == datasource_id
        else:
            assert stored_datasource.id is None
            assert retrieved_datasource.id is None
            assert retrieved_datasource.config["id"] is None

        # Make sure the name is populated correctly into the created datasource
        assert retrieved_datasource.name == datasource_name
        assert retrieved_datasource.config["name"] == datasource_name
