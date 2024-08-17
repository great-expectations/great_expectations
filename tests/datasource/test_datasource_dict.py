from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import pytest

from great_expectations.data_context.store import DatasourceStore
from great_expectations.datasource.datasource_dict import (
    CacheableDatasourceDict,
    DatasourceDict,
)
from great_expectations.datasource.fluent import PandasDatasource

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.ephemeral_data_context import (
        EphemeralDataContext,
    )


class DatasourceStoreSpy(DatasourceStore):
    def __init__(self, datasource_configs: list[dict] | None = None) -> None:
        self.list_keys_count = 0
        self.has_key_count = 0
        self.set_count = 0
        self.get_count = 0
        self.remove_key_count = 0

        super().__init__()

        datasource_configs = datasource_configs or []
        for config in datasource_configs:
            self.set(key=None, value=config)

        # Reset counters
        self.list_keys_count = 0
        self.has_key_count = 0
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

    def has_key(self, key) -> bool:
        self.has_key_count += 1
        return super().has_key(key)

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
def test_datasource_dict___setitem___(
    empty_datasource_dict: DatasourceDict, pandas_fds: PandasDatasource
):
    store = empty_datasource_dict._datasource_store
    assert store.set_count == 0

    empty_datasource_dict[pandas_fds.name] = pandas_fds
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
def test_datasource_dict___getitem___(
    build_datasource_dict_with_store_spy: Callable, pandas_fds: PandasDatasource
):
    datasource_dict = build_datasource_dict_with_store_spy(datasource_configs=[pandas_fds])
    store = datasource_dict._datasource_store
    assert store.get_count == 0

    retrieved_fds = datasource_dict[pandas_fds.name]
    assert store.get_count == 1
    assert retrieved_fds.dict() == pandas_fds.dict()


@pytest.fixture
def build_cacheable_datasource_dict_with_store_spy(
    in_memory_runtime_context: EphemeralDataContext,
) -> Callable:
    def _build_cacheable_datasource_dict_with_store_spy(
        datasource_configs: list[dict] | None = None,
        populate_cache: bool = True,
    ) -> CacheableDatasourceDict:
        datasource_dict = CacheableDatasourceDict(
            context=in_memory_runtime_context,
            datasource_store=DatasourceStoreSpy(datasource_configs=datasource_configs),
        )

        # Populate cache
        if populate_cache and datasource_configs:
            for ds in datasource_configs:
                datasource_dict.data[ds.name] = ds

        return datasource_dict

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
    datasource_dict = build_cacheable_datasource_dict_with_store_spy(
        datasource_configs=[pandas_fds]
    )
    return datasource_dict


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
    assert store.has_key_count == 0

    # Lookup will check store due to lack of presence in cache (but won't retrieve value)
    assert "my_fake_name" not in cacheable_datasource_dict_with_fds
    assert store.get_count == 0
    assert store.has_key_count == 1


@pytest.mark.unit
def test_cacheable_datasource_dict___setitem___(
    empty_cacheable_datasource_dict: CacheableDatasourceDict,
    pandas_fds: PandasDatasource,
):
    store = empty_cacheable_datasource_dict._datasource_store
    assert store.set_count == 0

    # FDS are not persisted with stores (only cache)
    empty_cacheable_datasource_dict[pandas_fds.name] = pandas_fds
    assert store.set_count == 0


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
        datasource_configs=[pandas_fds],
        populate_cache=False,
    )
    store = datasource_dict._datasource_store
    assert store.get_count == 0

    retrieved_fds = datasource_dict[pandas_fds.name]
    assert store.get_count == 1
    assert retrieved_fds.dict() == pandas_fds.dict()


@pytest.mark.unit
def test_cacheable_datasource_dict_set_datasource_adds_ids(
    build_cacheable_datasource_dict_with_store_spy: Callable,
    pandas_fds: PandasDatasource,
):
    datasource_dict = build_cacheable_datasource_dict_with_store_spy(
        datasource_configs=[pandas_fds],
        populate_cache=False,
    )

    assert pandas_fds.id is None
    updated_fds = datasource_dict.set_datasource(pandas_fds.name, pandas_fds)
    assert updated_fds.id is not None
