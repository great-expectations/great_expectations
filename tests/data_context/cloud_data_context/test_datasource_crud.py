"""This file is meant for integration tests related to datasource CRUD."""
import copy
from typing import TYPE_CHECKING, Callable, Type, Union
from unittest.mock import patch

import pytest

from great_expectations import DataContext
from great_expectations.core.serializer import (
    AbstractConfigSerializer,
    DictConfigSerializer,
)
from great_expectations.data_context import (
    AbstractDataContext,
    BaseDataContext,
    CloudDataContext,
)
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource import BaseDatasource, Datasource, LegacyDatasource
from great_expectations.datasource.datasource_serializer import (
    JsonDatasourceConfigSerializer,
)
from tests.data_context.conftest import MockResponse

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest


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
    shared_called_with_request_kwargs: dict,
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

    with patch(
        "requests.post", autospec=True, side_effect=mocked_datasource_post_response
    ) as mock_post, patch(
        "requests.get", autospec=True, side_effect=mocked_datasource_get_response
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
    shared_called_with_request_kwargs: dict,
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

    with patch(
        "requests.post", autospec=True, side_effect=mocked_datasource_post_response
    ) as mock_post, patch(
        "requests.get", autospec=True, side_effect=mocked_datasource_get_response
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
    shared_called_with_request_kwargs: dict,
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

    with patch(
        "requests.post", autospec=True, side_effect=mocked_datasource_post_response
    ) as mock_post, patch(
        "requests.get", autospec=True, side_effect=mocked_datasource_get_response
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

        data_connector_name = tuple(stored_datasource.data_connectors.keys())[0]
        stored_data_connector = stored_datasource.data_connectors[data_connector_name]

        # Make sure the id was populated correctly into the created datasource object and config
        assert stored_datasource.id == fake_datasource_id
        assert stored_data_connector.id == fake_data_connector_id

        # Make sure the name is populated correctly into the created datasource
        assert stored_datasource.name == datasource_name
        assert stored_datasource.config["name"] == datasource_name
        assert stored_data_connector.name == data_connector_name


@pytest.mark.unit
def test_non_cloud_backed_data_context_save_datasource_empty_store(
    empty_data_context: DataContext,
    datasource_config_with_names: DatasourceConfig,
):
    context: DataContext = empty_data_context
    # Make sure the fixture has the right configuration
    assert len(context.list_datasources()) == 0

    datasource: Datasource = context._build_datasource_from_config(
        datasource_config_with_names
    )
    datasource_name: str = datasource.name

    with patch(
        "great_expectations.data_context.store.datasource_store.DatasourceStore.set",
        autospec=True,
        return_value=datasource_config_with_names,
    ):
        updated_datasource: Union[
            LegacyDatasource, BaseDatasource
        ] = context.save_datasource(datasource)

        # Make sure the datasource config got into the context config
        assert len(context.config.datasources) == 1
        assert (
            context.config.datasources[datasource_name] == datasource_config_with_names
        )

        cached_datasource = context._cached_datasources[datasource_name]

        # Make sure the datasource got into the cache
        assert len(context._cached_datasources) == 1

        # Make sure the stored and returned datasource is the same one as the cached datasource
        assert id(updated_datasource) == id(cached_datasource)
        assert updated_datasource == cached_datasource

        # Make sure the stored and returned datasource is a different instance
        assert not id(cached_datasource) == id(datasource)
        assert not id(updated_datasource) == id(datasource)
        assert not cached_datasource == datasource
        assert not updated_datasource == datasource

        # Make sure the stored and returned datasource are otherwise equal
        serializer: AbstractConfigSerializer = DictConfigSerializer(
            schema=datasourceConfigSchema
        )
        updated_datasource_dict = serializer.serialize(
            datasourceConfigSchema.load(updated_datasource.config)
        )
        orig_datasource_dict = serializer.serialize(
            datasourceConfigSchema.load(datasource.config)
        )

        for attribute in ("name", "execution_engine", "data_connectors"):
            assert updated_datasource_dict[attribute] == orig_datasource_dict[attribute]


@pytest.mark.unit
def test_non_cloud_backed_data_context_save_datasource_overwrite_existing(
    empty_data_context: DataContext,
    datasource_config_with_names: DatasourceConfig,
):
    """What does this test and why? This ensures there are no checks that stop an overwrite of an existing datasource in context.save_datasource(). It does not test the underlying store or store backend."""
    context: DataContext = empty_data_context
    # Make sure the fixture has the right configuration
    assert len(context.list_datasources()) == 0

    # 1. Add datasource to empty context
    serializer: AbstractConfigSerializer = DictConfigSerializer(
        schema=datasourceConfigSchema
    )
    datasource_config_with_names_dict = serializer.serialize(
        datasource_config_with_names
    )
    context.add_datasource(**datasource_config_with_names_dict)
    assert len(context.list_datasources()) == 1

    # 2. Create new datasource (slightly different, but same name) and call context.save_datasource()
    datasource_config_with_names_modified = copy.deepcopy(datasource_config_with_names)
    data_connector_name: str = tuple(
        datasource_config_with_names_modified.data_connectors.keys()
    )[0]
    data_connector_config = copy.deepcopy(
        datasource_config_with_names_modified.data_connectors[data_connector_name]
    )
    datasource_config_with_names_modified.data_connectors.pop(data_connector_name, None)

    # Rename the data connector
    new_data_connector_name: str = "new_data_connector_name"
    data_connector_config["name"] = new_data_connector_name
    datasource_config_with_names_modified.data_connectors[
        new_data_connector_name
    ] = data_connector_config

    new_datasource: Datasource = context._build_datasource_from_config(
        datasource_config_with_names_modified
    )

    orig_datasource_name: str = datasource_config_with_names.name
    datasource_name: str = new_datasource.name
    assert orig_datasource_name == datasource_name

    pre_update_datasource = context.get_datasource(datasource_name)
    assert tuple(pre_update_datasource.data_connectors.keys())[0] == data_connector_name

    # 3. Make sure no exceptions are raised when saving.
    with patch(
        "great_expectations.data_context.store.datasource_store.DatasourceStore.set",
        autospec=True,
        return_value=datasource_config_with_names_modified,
    ):

        updated_datasource: Union[
            LegacyDatasource, BaseDatasource
        ] = context.save_datasource(new_datasource)

        # Make sure the datasource config got into the context config
        assert len(context.config.datasources) == 1
        assert (
            context.config.datasources[datasource_name]
            == datasource_config_with_names_modified
        )

        cached_datasource = context._cached_datasources[datasource_name]

        # Make sure the datasource did not get duplicated in the cache
        assert len(context._cached_datasources) == 1

        # Make sure the name was updated
        updated_datasource_data_connector_name: str = tuple(
            updated_datasource.data_connectors.keys()
        )[0]
        assert updated_datasource_data_connector_name == new_data_connector_name

        # Make sure the stored and returned datasource is the same one as the cached datasource
        assert id(updated_datasource) == id(cached_datasource)
        assert updated_datasource == cached_datasource

        # Make sure the stored and returned datasource is a different instance
        assert not id(cached_datasource) == id(new_datasource)
        assert not id(updated_datasource) == id(new_datasource)
        assert not cached_datasource == new_datasource
        assert not updated_datasource == new_datasource

        # Make sure the stored and returned datasource are otherwise equal
        serializer: AbstractConfigSerializer = DictConfigSerializer(
            schema=datasourceConfigSchema
        )
        updated_datasource_dict = serializer.serialize(
            datasourceConfigSchema.load(updated_datasource.config)
        )
        orig_datasource_dict = serializer.serialize(
            datasourceConfigSchema.load(new_datasource.config)
        )

        for attribute in ("name", "execution_engine", "data_connectors"):
            assert updated_datasource_dict[attribute] == orig_datasource_dict[attribute]


@pytest.mark.parametrize(
    "data_context_fixture_name,data_context_type",
    [
        pytest.param(
            "empty_base_data_context_in_cloud_mode",
            BaseDataContext,
            id="BaseDataContext",
        ),
        pytest.param(
            "empty_data_context_in_cloud_mode",
            DataContext,
            id="DataContext",
        ),
        pytest.param(
            "empty_cloud_data_context",
            CloudDataContext,
            id="CloudDataContext",
        ),
    ],
)
@pytest.mark.cloud
@pytest.mark.unit
def test_cloud_backed_data_context_save_datasource_empty_store(
    data_context_fixture_name: str,
    data_context_type: Type[AbstractDataContext],
    datasource_config_with_names: DatasourceConfig,
    datasource_config_with_names_and_ids: DatasourceConfig,
    request: "FixtureRequest",
):
    """Any Data Context in cloud mode should save to the cloud backed Datasource store when calling save_datasource. When saving, it should use the id from the response to create the datasource, and update both the config and cache."""

    context: Union[
        BaseDataContext, DataContext, CloudDataContext
    ] = request.getfixturevalue(data_context_fixture_name)

    # Make sure the fixture has the right configuration
    assert isinstance(context, data_context_type)
    assert context.ge_cloud_mode
    assert len(context.list_datasources()) == 0

    datasource: Datasource = context._build_datasource_from_config(
        datasource_config_with_names
    )
    datasource_name: str = datasource.name
    data_connector_name: str = tuple(datasource.data_connectors.keys())[0]

    with patch(
        "great_expectations.data_context.store.datasource_store.DatasourceStore.set",
        autospec=True,
        return_value=datasource_config_with_names_and_ids,
    ):
        updated_datasource: Union[
            LegacyDatasource, BaseDatasource
        ] = context.save_datasource(datasource)

        # Make sure the datasource config got into the context config
        assert len(context.config.datasources) == 1
        assert (
            context.config.datasources[datasource_name]
            == datasource_config_with_names_and_ids
        )

        cached_datasource = context._cached_datasources[datasource_name]

        # Make sure the datasource got into the cache
        assert len(context._cached_datasources) == 1

        # Make sure the stored and returned datasource is the same one as the cached datasource
        assert id(updated_datasource) == id(cached_datasource)
        assert updated_datasource == cached_datasource

        # Make sure the stored and returned datasource is a different instance (now has ids)
        assert not id(cached_datasource) == id(datasource)
        assert not id(updated_datasource) == id(datasource)
        assert not cached_datasource == datasource
        assert not updated_datasource == datasource

        # Make sure the stored and returned datasource are otherwise equal
        serializer: AbstractConfigSerializer = DictConfigSerializer(
            schema=datasourceConfigSchema
        )
        updated_datasource_dict = serializer.serialize(
            datasourceConfigSchema.load(updated_datasource.config)
        )
        orig_datasource_dict = serializer.serialize(
            datasourceConfigSchema.load(datasource.config)
        )

        for attribute in (
            "name",
            "execution_engine",
        ):
            assert updated_datasource_dict[attribute] == orig_datasource_dict[attribute]

        updated_datasource_dict_no_datasource_id = copy.deepcopy(
            updated_datasource_dict
        )
        updated_datasource_dict_no_datasource_id["data_connectors"][
            data_connector_name
        ].pop("id", None)
        assert (
            updated_datasource_dict_no_datasource_id["data_connectors"]
            == orig_datasource_dict["data_connectors"]
        )

        # Make sure that the id is populated only in the updated and cached datasource
        assert datasource.id is None
        assert datasource.data_connectors[data_connector_name].id is None
        assert updated_datasource.id == datasource_config_with_names_and_ids.id
        assert (
            updated_datasource.data_connectors[data_connector_name].id
            == datasource_config_with_names_and_ids.data_connectors[
                data_connector_name
            ]["id"]
        )
