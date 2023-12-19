from __future__ import annotations

import pathlib
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations.data_context import get_context
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
    GXCloudConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.datasource import Datasource
from great_expectations.exceptions import DatasourceNotFoundError


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


@pytest.mark.unit
def test_get_datasource_retrieves_from_cache(
    in_memory_runtime_context,
) -> None:
    """
    What does this test and why?

    For non-Cloud contexts, we should always be looking at the cache for object retrieval.
    """
    context = in_memory_runtime_context

    name = context.list_datasources()[0]["name"]

    # If the value is in the cache, no store methods should be invoked
    with mock.patch(
        "great_expectations.data_context.store.DatasourceStore.get"
    ) as mock_get:
        context.get_datasource(name)

    assert not mock_get.called


@pytest.mark.unit
def test_get_datasource_cache_miss(in_memory_runtime_context) -> None:
    """
    What does this test and why?

    For all non-Cloud contexts, we should leverage the underlying store in the case
    of a cache miss.
    """
    context = in_memory_runtime_context

    name = "my_fake_datasource_name"

    # Initial GET will miss the cache, necessitating store retrieval
    with mock.patch(
        "great_expectations.core.datasource_dict.DatasourceDict.__getitem__"
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
def test_BaseDataContext_update_datasource_updates_existing_data_source(
    in_memory_runtime_context: EphemeralDataContext,
    pandas_enabled_datasource_config: dict,
) -> None:
    """
    What does this test and why?

    Updating a Data Source should update a Data Source
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

    context.update_datasource(datasource)

    retrieved_datasource = context.get_datasource(datasource_name=name)
    assert retrieved_datasource.data_connectors.keys() == data_connectors.keys()


@pytest.mark.unit
def test_BaseDataContext_update_datasource_fails_when_datsource_does_not_exist(
    in_memory_runtime_context: EphemeralDataContext,
    pandas_enabled_datasource_config: dict,
) -> None:
    """
    What does this test and why?

    Updating a data source that does not exist should create a new data source.
    """
    context = in_memory_runtime_context

    name = pandas_enabled_datasource_config["name"]
    pandas_enabled_datasource_config.pop("class_name")
    datasource = Datasource(**pandas_enabled_datasource_config)

    assert name not in context.datasources

    with pytest.raises(DatasourceNotFoundError):
        context.update_datasource(datasource)


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
