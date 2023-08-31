from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import pytest

from great_expectations.core.datasource_dict import (
    CacheableDatasourceDict,
    DatasourceDict,
)
from great_expectations.core.serializer import DictConfigSerializer
from great_expectations.data_context.store import DatasourceStore
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource.fluent import PandasDatasource
from great_expectations.datasource.new_datasource import Datasource

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.ephemeral_data_context import (
        EphemeralDataContext,
    )


class DatasourceStoreSpy(DatasourceStore):
    def __init__(self, datasource_configs: list[dict] | None = None) -> None:
        self.list_keys_count = 0
        self.set_count = 0
        self.get_count = 0
        self.remove_key_count = 0

        super().__init__(serializer=DictConfigSerializer(schema=datasourceConfigSchema))

        datasource_configs = datasource_configs or []
        for config in datasource_configs:
            self.set(key=None, value=config)

        # Reset counters
        self.list_keys_count = 0
        self.set_count = 0
        self.get_count = 0
        self.remove_key_count = 0

    def get(self, key):
        self.get_count += 1
        return super().get(key)

    def set(self, key, value, **kwargs):
        self.set_count += 1
        return super().set(key, value, **kwargs)

    def list_keys(self):
        self.list_keys_count += 1
        return super().list_keys()

    def remove_key(self, key):
        self.remove_key_count += 1
        return super().remove_key(key)


@pytest.fixture
def build_datasource_dict_with_store_spy(
    in_memory_runtime_context: EphemeralDataContext,
) -> Callable:
    def _build_datasource_dict_with_store_spy(
        datasource_configs: list[dict] | None = None,
    ) -> DatasourceDict:
        return DatasourceDict(
            context=in_memory_runtime_context,
            datasource_store=DatasourceStoreSpy(datasource_configs=datasource_configs),
        )

    return _build_datasource_dict_with_store_spy


@pytest.fixture
def empty_datasource_dict(
    build_datasource_dict_with_store_spy: Callable,
) -> DatasourceDict:
    return build_datasource_dict_with_store_spy()


@pytest.fixture
def pandas_fds_name() -> str:
    return "my_pandas_fds"


@pytest.fixture
def pandas_fds(pandas_fds_name: str) -> PandasDatasource:
    return PandasDatasource(name=pandas_fds_name)


@pytest.fixture
def pandas_block_datasource() -> Datasource:
    return Datasource(
        name="my_block_pandas",
        data_connectors={
            "my_data_connector": {
                "assets": {
                    "my_asset": {
                        "base_directory": "./data",
                        "class_name": "Asset",
                        "glob_directive": "*.csv",
                        "group_names": ["filename"],
                        "module_name": "great_expectations.datasource.data_connector.asset",
                        "pattern": "(.*)\\.csv",
                        "reader_options": {"delimiter": ","},
                    }
                },
                "base_directory": "./data",
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
            }
        },
        execution_engine={
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
    )


@pytest.fixture
def pandas_block_datasource_config(
    pandas_block_datasource: Datasource,
) -> DatasourceConfig:
    config = pandas_block_datasource.config
    config["name"] = pandas_block_datasource.name
    return datasourceConfigSchema.load(config)


@pytest.mark.unit
def test_datasource_dict_data_property_requests_store_just_in_time(
    empty_datasource_dict: DatasourceDict,
):
    store = empty_datasource_dict._datasource_store

    store.list_keys_count = 0
    _ = empty_datasource_dict.data
    store.list_keys_count = 1


@pytest.mark.unit
def test_datasource_dict___contains___requests_store_just_in_time(
    empty_datasource_dict: DatasourceDict,
):
    store = empty_datasource_dict._datasource_store

    store.list_keys_count = 0
    _ = "foo" in empty_datasource_dict.data
    store.list_keys_count = 1


@pytest.mark.unit
def test_datasource_dict___setitem___with_fds(
    empty_datasource_dict: DatasourceDict, pandas_fds: PandasDatasource
):
    store = empty_datasource_dict._datasource_store
    assert store.set_count == 0

    empty_datasource_dict[pandas_fds.name] = pandas_fds
    assert store.set_count == 1


@pytest.mark.unit
def test_datasource_dict___setitem___with_block_datasource(
    empty_datasource_dict: DatasourceDict, pandas_block_datasource: Datasource
):
    store = empty_datasource_dict._datasource_store
    assert store.set_count == 0

    empty_datasource_dict[pandas_block_datasource.name] = pandas_block_datasource
    assert store.set_count == 1


@pytest.mark.unit
def test_datasource_dict___delitem__raises_key_error_on_store_miss(
    empty_datasource_dict: DatasourceDict,
):
    store = empty_datasource_dict._datasource_store
    assert store.remove_key_count == 0

    with pytest.raises(KeyError):
        empty_datasource_dict["my_nonxxistent_ds"]
    assert store.remove_key_count == 0

    with pytest.raises(KeyError):
        empty_datasource_dict.pop("my_nonexistent_ds")
    assert store.remove_key_count == 0


@pytest.mark.unit
def test_datasource_dict___getitem__raises_key_error_on_store_miss(
    empty_datasource_dict: DatasourceDict,
):
    with pytest.raises(KeyError):
        empty_datasource_dict["my_nonexistent_ds"]


@pytest.mark.unit
def test_datasource_dict___getitem___with_fds(
    build_datasource_dict_with_store_spy: Callable, pandas_fds: PandasDatasource
):
    datasource_dict = build_datasource_dict_with_store_spy(
        datasource_configs=[pandas_fds]
    )
    store = datasource_dict._datasource_store
    assert store.get_count == 0

    retrieved_fds = datasource_dict[pandas_fds.name]
    assert store.get_count == 1
    assert retrieved_fds.dict() == pandas_fds.dict()


@pytest.mark.unit
def test_datasource_dict___getitem___with_block_datasource(
    build_datasource_dict_with_store_spy: Callable, pandas_block_datasource_config: dict
):
    datasource_dict = build_datasource_dict_with_store_spy(
        datasource_configs=[pandas_block_datasource_config]
    )
    store = datasource_dict._datasource_store
    assert store.get_count == 0

    retrieved_ds = datasource_dict[pandas_block_datasource_config["name"]]
    assert store.get_count == 1

    # Compare arbitrary nested value
    assert (
        retrieved_ds.config["data_connectors"].keys()
        == pandas_block_datasource_config["data_connectors"].keys()
    )


@pytest.fixture
def build_cacheable_datasource_dict_with_store_spy(
    in_memory_runtime_context: EphemeralDataContext,
) -> Callable:
    def _build_cacheable_datasource_dict_with_store_spy(
        datasource_configs: list[dict] | None = None,
    ) -> CacheableDatasourceDict:
        return CacheableDatasourceDict(
            context=in_memory_runtime_context,
            datasource_store=DatasourceStoreSpy(datasource_configs=datasource_configs),
        )

    return _build_cacheable_datasource_dict_with_store_spy


@pytest.fixture
def empty_cacheable_datasource_dict(
    build_cacheable_datasource_dict_with_store_spy: Callable,
) -> CacheableDatasourceDict:
    return build_cacheable_datasource_dict_with_store_spy()


@pytest.fixture
def cacheable_datasource_dict_with_fds(
    build_cacheable_datasource_dict_with_store_spy: Callable,
    pandas_fds: PandasDatasource,
) -> CacheableDatasourceDict:
    return build_cacheable_datasource_dict_with_store_spy(
        datasource_configs=[pandas_fds]
    )


@pytest.mark.unit
def test_cacheable_datasource_dict___contains___uses_cache(
    cacheable_datasource_dict_with_fds: CacheableDatasourceDict, pandas_fds_name: str
):
    store = cacheable_datasource_dict_with_fds._datasource_store

    assert store.get_count == 0
    # Lookup will not check store due to presence in cache
    assert pandas_fds_name in cacheable_datasource_dict_with_fds
    assert store.get_count == 0


@pytest.mark.unit
def test_cacheable_datasource_dict___contains___requests_store_upon_cache_miss(
    cacheable_datasource_dict_with_fds: CacheableDatasourceDict,
):
    store = cacheable_datasource_dict_with_fds._datasource_store

    assert store.get_count == 0
    assert store.list_keys_count == 0

    # Lookup will check store due to lack of presence in cache (but won't retrieve value)
    assert "my_fake_name" not in cacheable_datasource_dict_with_fds
    assert store.get_count == 0
    assert store.list_keys_count == 1


@pytest.mark.unit
def test_cacheable_datasource_dict___setitem___with_fds(
    empty_cacheable_datasource_dict: CacheableDatasourceDict,
    pandas_fds: PandasDatasource,
):
    store = empty_cacheable_datasource_dict._datasource_store
    assert store.set_count == 0

    # FDS are not persisted with stores (only cache)
    empty_cacheable_datasource_dict[pandas_fds.name] = pandas_fds
    assert store.set_count == 0


@pytest.mark.unit
def test_cacheable_datasource_dict___setitem___with_block_datasource(
    empty_cacheable_datasource_dict: DatasourceDict, pandas_block_datasource: Datasource
):
    store = empty_cacheable_datasource_dict._datasource_store
    assert store.set_count == 0

    # non-FDS use both store and cache
    empty_cacheable_datasource_dict[
        pandas_block_datasource.name
    ] = pandas_block_datasource
    assert store.set_count == 1


@pytest.mark.unit
def test_cacheable_datasource_dict___delitem__updates_both_cache_and_store(
    cacheable_datasource_dict_with_fds: CacheableDatasourceDict, pandas_fds_name: str
):
    store = cacheable_datasource_dict_with_fds._datasource_store
    assert store.remove_key_count == 0

    # Deletion will go down to the store level
    del cacheable_datasource_dict_with_fds[pandas_fds_name]
    assert store.remove_key_count == 1

    # Should also impact the cache
    assert pandas_fds_name not in cacheable_datasource_dict_with_fds.data


@pytest.mark.unit
def test_cacheable_datasource_dict___delitem__raises_key_error_on_store_miss(
    empty_cacheable_datasource_dict: CacheableDatasourceDict,
):
    store = empty_cacheable_datasource_dict._datasource_store
    assert store.remove_key_count == 0

    with pytest.raises(KeyError):
        empty_cacheable_datasource_dict["my_nonxxistent_ds"]
    assert store.remove_key_count == 0

    with pytest.raises(KeyError):
        empty_cacheable_datasource_dict.pop("my_nonexistent_ds")
    assert store.remove_key_count == 0


@pytest.mark.unit
def test_cacheable_datasource_dict___getitem__raises_key_error_on_store_miss(
    empty_cacheable_datasource_dict: CacheableDatasourceDict,
):
    with pytest.raises(KeyError):
        empty_cacheable_datasource_dict["my_nonexistent_ds"]


@pytest.mark.unit
def test_cacheable_datasource_dict___getitem___with_fds(
    build_cacheable_datasource_dict_with_store_spy: Callable,
    pandas_fds: PandasDatasource,
):
    datasource_dict = build_cacheable_datasource_dict_with_store_spy(
        datasource_configs=[pandas_fds]
    )
    store = datasource_dict._datasource_store
    assert store.get_count == 0

    retrieved_fds = datasource_dict[pandas_fds.name]
    assert store.get_count == 1
    assert retrieved_fds.dict() == pandas_fds.dict()


@pytest.mark.unit
def test_cacheable_datasource_dict___getitem___with_block_datasource(
    build_cacheable_datasource_dict_with_store_spy: Callable,
    pandas_block_datasource_config: dict,
):
    datasource_dict = build_cacheable_datasource_dict_with_store_spy(
        datasource_configs=[pandas_block_datasource_config]
    )
    store = datasource_dict._datasource_store
    assert store.get_count == 0

    retrieved_ds = datasource_dict[pandas_block_datasource_config["name"]]
    assert store.get_count == 1

    # Compare arbitrary nested value
    assert (
        retrieved_ds.config["data_connectors"].keys()
        == pandas_block_datasource_config["data_connectors"].keys()
    )
