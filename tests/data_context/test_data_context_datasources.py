from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)
from great_expectations.data_context.store.inline_store_backend import (
    InlineStoreBackend,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    GXCloudConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.datasource import Datasource
from great_expectations.util import get_context

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


@pytest.fixture
def pandas_enabled_datasource_config() -> dict:
    name = "my_pandas_datasource"
    class_name = "Datasource"
    execution_engine = {
        "class_name": "PandasExecutionEngine",
    }
    data_connectors = {
        "my_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "../data/",
            "default_regex": {
                "group_names": ["data_asset_name"],
                "pattern": "(.*)",
            },
        },
    }

    config = {
        "name": name,
        "class_name": class_name,
        "execution_engine": execution_engine,
        "data_connectors": data_connectors,
    }
    return config


@pytest.mark.integration
@pytest.mark.cloud
def test_data_context_instantiates_gx_cloud_store_backend_with_cloud_config(
    tmp_path: pathlib.Path,
    data_context_config_with_datasources: DataContextConfig,
    ge_cloud_config: GXCloudConfig,
) -> None:
    project_path = tmp_path / "my_data_context"
    project_path.mkdir()

    # Clear datasources to improve test performance in DataContext._init_datasources
    data_context_config_with_datasources.datasources = {}

    context = get_context(
        project_config=data_context_config_with_datasources,
        context_root_dir=str(project_path),
        cloud_base_url=ge_cloud_config.base_url,
        cloud_access_token=ge_cloud_config.access_token,
        cloud_organization_id=ge_cloud_config.organization_id,
        cloud_mode=True,
    )

    assert isinstance(context._datasource_store.store_backend, GXCloudStoreBackend)


@pytest.mark.filesystem
@pytest.mark.integration
def test_data_context_instantiates_inline_store_backend_with_filesystem_config(
    tmp_path: pathlib.Path,
    data_context_config_with_datasources: DataContextConfig,
) -> None:
    project_path = tmp_path / "my_data_context"
    project_path.mkdir()

    # Clear datasources to improve test performance in DataContext._init_datasources
    data_context_config_with_datasources.datasources = {}

    context = get_context(
        project_config=data_context_config_with_datasources,
        context_root_dir=str(project_path),
        cloud_mode=False,
    )

    assert isinstance(context._datasource_store.store_backend, InlineStoreBackend)


@pytest.mark.parametrize(
    "data_context_fixture_name",
    [
        # In order to leverage existing fixtures in parametrization, we provide
        # their string names and dynamically retrieve them using pytest's built-in
        # `request` fixture.
        # Source: https://stackoverflow.com/a/64348247
        pytest.param(
            "in_memory_runtime_context",
            id="BaseDataContext",
        ),
        pytest.param(
            "cloud_data_context_in_cloud_mode_with_datasource_pandas_engine",
            id="DataContext",
        ),
    ],
)
@pytest.mark.unit
def test_get_datasource_retrieves_from_cache(
    data_context_fixture_name: str,
    request,
) -> None:
    """
    What does this test and why?

    For both persistence-enabled and disabled contexts, we should always be looking at the
    cache for object retrieval.
    """
    context = request.getfixturevalue(data_context_fixture_name)

    name = context.list_datasources()[0]["name"]

    # If the value is in the cache, no store methods should be invoked
    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.get"
    ) as mock_get:
        context.get_datasource(name)

    assert not mock_get.called


@pytest.mark.parametrize(
    "data_context_fixture_name",
    [
        # In order to leverage existing fixtures in parametrization, we provide
        # their string names and dynamically retrieve them using pytest's built-in
        # `request` fixture.
        # Source: https://stackoverflow.com/a/64348247
        pytest.param(
            "in_memory_runtime_context",
            id="BaseDataContext",
        ),
        pytest.param(
            "cloud_data_context_in_cloud_mode_with_datasource_pandas_engine",
            id="DataContext",
        ),
    ],
)
@pytest.mark.unit
def test_get_datasource_cache_miss(
    data_context_fixture_name: str,
    request,
) -> None:
    """
    What does this test and why?

    For all contexts, we should leverage the underlying store in the case
    of a cache miss.
    """
    context = request.getfixturevalue(data_context_fixture_name)

    name = "my_fake_datasource_name"

    # Initial GET will miss the cache, necessitating store retrieval
    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.has_key"
    ), mock.patch(
        "great_expectations.data_context.data_context.AbstractDataContext._instantiate_datasource_from_config"
    ), mock.patch(
        "great_expectations.data_context.types.base.datasourceConfigSchema.load",
    ), mock.patch(
        "great_expectations.data_context.store.DatasourceStore.get"
    ) as mock_get:
        context.get_datasource(name)

    assert mock_get.called

    # Subsequent GET will retrieve from the cache
    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.get"
    ) as mock_get:
        context.get_datasource(name)

    assert not mock_get.called


@pytest.mark.unit
@pytest.mark.cloud
def test_DataContext_add_datasource_updates_cache_and_store(
    cloud_data_context_in_cloud_mode_with_datasource_pandas_engine: CloudDataContext,
    datasource_config_with_names: DatasourceConfig,
) -> None:
    """
    What does this test and why?

    For persistence-enabled contexts, we should update both the cache and the underlying
    store upon adding a datasource.

    Note: the actual datasource config is not important for this test, it just needs to be a valid config since
        initialize=True must be set in add_datasource() to add the datasource to the cache.
    """
    context = cloud_data_context_in_cloud_mode_with_datasource_pandas_engine

    name = "some_random_name"
    datasource_config_with_names.name = name

    assert name not in context.datasources

    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.set",
        autospec=True,
        return_value=datasource_config_with_names,
    ) as mock_set, pytest.deprecated_call():  # non-FDS datasources are discouraged in Cloud
        context.add_datasource(**datasource_config_with_names.to_json_dict())

    mock_set.assert_called_once()
    assert name in context.datasources


@pytest.mark.unit
@pytest.mark.cloud
def test_DataContext_update_datasource_updates_existing_value_in_cache_and_store(
    cloud_data_context_in_cloud_mode_with_datasource_pandas_engine: CloudDataContext,
    pandas_enabled_datasource_config: dict,
) -> None:
    """
    What does this test and why?

    For persistence-enabled contexts, we should update both the cache and the underlying
    store upon updating an existing datasource.
    """
    context = cloud_data_context_in_cloud_mode_with_datasource_pandas_engine

    name = context.list_datasources()[0]["name"]
    pandas_enabled_datasource_config["name"] = name
    data_connectors = pandas_enabled_datasource_config["data_connectors"]
    pandas_enabled_datasource_config.pop("class_name")
    datasource = Datasource(**pandas_enabled_datasource_config)

    assert name in context.datasources

    # Ensure that our cache value is updated to reflect changes
    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.has_key"
    ), mock.patch(
        "great_expectations.data_context.store.DatasourceStore.update"
    ) as mock_update:
        context.update_datasource(datasource)

    mock_update.assert_called_once()
    assert name in context.datasources

    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.get"
    ) as mock_get:
        retrieved_datasource = context.get_datasource(datasource_name=name)

    assert not mock_get.called
    assert retrieved_datasource.data_connectors.keys() == data_connectors.keys()


@pytest.mark.unit
@pytest.mark.cloud
def test_DataContext_update_datasource_creates_new_value_in_cache_and_store(
    cloud_data_context_in_cloud_mode_with_datasource_pandas_engine: CloudDataContext,
    pandas_enabled_datasource_config: dict,
) -> None:
    """
    What does this test and why?

    For persistence-enabled contexts, we should update both the cache and the underlying
    store upon using update to create a new datasource.
    """
    context = cloud_data_context_in_cloud_mode_with_datasource_pandas_engine

    name = pandas_enabled_datasource_config["name"]
    pandas_enabled_datasource_config.pop("class_name")
    datasource = Datasource(**pandas_enabled_datasource_config)

    assert name not in context.datasources

    # Ensure that a brand new cache value is added to reflect changes
    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.has_key"
    ), mock.patch(
        "great_expectations.data_context.store.DatasourceStore.update"
    ) as mock_update:
        context.update_datasource(datasource)

    mock_update.assert_called_once()
    assert name in context.datasources


@pytest.mark.unit
@pytest.mark.cloud
def test_DataContext_delete_datasource_updates_cache(
    cloud_data_context_in_cloud_mode_with_datasource_pandas_engine: CloudDataContext,
) -> None:
    """
    What does this test and why?

    For persistence-enabled contexts, we should delete values in both the cache and the
    underlying store.
    """
    context = cloud_data_context_in_cloud_mode_with_datasource_pandas_engine

    name = context.list_datasources()[0]["name"]

    # If the value is in the cache, no store methods should be invoked
    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.remove_key"
    ) as mock_delete:
        context.delete_datasource(name)

    mock_delete.assert_called_once()
    assert name not in context.datasources


@pytest.mark.unit
def test_BaseDataContext_add_datasource_updates_cache(
    in_memory_runtime_context: EphemeralDataContext,
    pandas_enabled_datasource_config: dict,
) -> None:
    """
    What does this test and why?

    For persistence-disabled contexts, we should only update the cache upon adding a
    datasource.
    """
    context = in_memory_runtime_context

    name = pandas_enabled_datasource_config["name"]

    assert name not in context.datasources

    context.add_datasource(**pandas_enabled_datasource_config)

    assert name in context.datasources


@pytest.mark.unit
def test_BaseDataContext_update_datasource_updates_existing_value_in_cache(
    in_memory_runtime_context: EphemeralDataContext,
    pandas_enabled_datasource_config: dict,
) -> None:
    """
    What does this test and why?

    When save_changes is set to False, we should only update the cache upon
    updating an existing datasource.
    """
    context = in_memory_runtime_context

    name = context.list_datasources()[0]["name"]
    pandas_enabled_datasource_config["name"] = name
    data_connectors = pandas_enabled_datasource_config["data_connectors"]
    pandas_enabled_datasource_config.pop("class_name")
    datasource = Datasource(**pandas_enabled_datasource_config)

    assert name in context.datasources
    cached_datasource = context.datasources[name]
    assert cached_datasource.data_connectors.keys() != data_connectors.keys()

    with pytest.deprecated_call():
        # Ensure that our cache value is updated to reflect changes
        context.update_datasource(datasource, save_changes=False)

    assert name in context.datasources
    cached_datasource = context.datasources[name]
    assert cached_datasource.data_connectors.keys() == data_connectors.keys()

    retrieved_datasource = context.get_datasource(datasource_name=name)
    assert retrieved_datasource.data_connectors.keys() == data_connectors.keys()


@pytest.mark.unit
def test_BaseDataContext_update_datasource_creates_new_value_in_cache(
    in_memory_runtime_context: EphemeralDataContext,
    pandas_enabled_datasource_config: dict,
) -> None:
    """
    What does this test and why?

    When save_changes is set to False, we should only update the cache upon
    using update to create a new datasource.
    """
    context = in_memory_runtime_context

    name = pandas_enabled_datasource_config["name"]
    pandas_enabled_datasource_config.pop("class_name")
    datasource = Datasource(**pandas_enabled_datasource_config)

    assert name not in context.datasources

    # Ensure that a brand new cache value is added to reflect changes
    with pytest.deprecated_call():
        context.update_datasource(datasource, save_changes=False)

    assert name in context.datasources


@pytest.mark.unit
def test_BaseDataContext_delete_datasource_updates_cache(
    in_memory_runtime_context: EphemeralDataContext,
) -> None:
    """
    What does this test and why?

    When save_changes is set to False, we should only delete the value from
    the cache.
    """
    context = in_memory_runtime_context

    name = context.list_datasources()[0]["name"]

    # If the value is in the cache, no store methods should be invoked
    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.remove_key"
    ) as mock_delete, pytest.deprecated_call():
        context.delete_datasource(name, save_changes=False)

    assert not mock_delete.called
    assert name not in context.datasources


@pytest.mark.unit
def test_list_datasources() -> None:
    project_config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults()
    )
    project_config.datasources = {
        "my_datasource_name": {
            "class_name": "Datasource",
            "data_connectors": {},
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
        }
    }
    context = gx.get_context(project_config=project_config)

    datasource_name = "my_experimental_datasource_awaiting_migration"
    context.sources.add_pandas(datasource_name)

    assert len(context.list_datasources()) == 2


@pytest.mark.filesystem
@pytest.mark.integration
def test_get_available_data_assets_names(empty_data_context) -> None:
    datasource_name = "my_fluent_pandas_datasource"
    datasource = empty_data_context.sources.add_pandas(datasource_name)
    asset_name = "test_data_frame"
    datasource.add_dataframe_asset(name=asset_name)

    assert len(empty_data_context.get_available_data_asset_names()) == 1

    data_asset_names = dict(
        empty_data_context.get_available_data_asset_names(
            datasource_names=datasource_name
        )
    )

    assert asset_name in data_asset_names[datasource_name]
