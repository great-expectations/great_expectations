from __future__ import annotations

import logging
from collections import UserDict
from typing import TYPE_CHECKING, Protocol, TypeVar, overload, runtime_checkable

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource.fluent import Datasource as FluentDatasource
from great_expectations.datasource.fluent.constants import _IN_MEMORY_DATA_ASSET_TYPE

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.data_context.store.datasource_store import DatasourceStore
    from great_expectations.datasource.fluent.interfaces import DataAsset
    from great_expectations.datasource.new_datasource import (
        BaseDatasource,
    )

T = TypeVar("T", bound=FluentDatasource)

logger = logging.getLogger(__name__)


@runtime_checkable
class SupportsInMemoryDataAssets(Protocol):
    @property
    def assets(self) -> list[DataAsset]:
        ...

    def add_dataframe_asset(self, **kwargs) -> DataAsset:
        ...


class DatasourceDict(UserDict):
    """
    An abstraction around the DatasourceStore to enable easy retrieval and storage of Datasource objects
    using dictionary syntactic sugar.

    Example:
    ```
    d = DatasourceDict(...)

    d["my_fds"] = pandas_fds # Underlying DatasourceStore makes a `set()` call
    pandas_fds = d["my_fds"] # Underlying DatasourceStore makes a `get()` call
    ```
    """

    def __init__(
        self,
        context: AbstractDataContext,
        datasource_store: DatasourceStore,
    ):
        self._context = context  # If possible, we should avoid passing the context through - once block-style is removed, we can extract this
        self._datasource_store = datasource_store
        self._in_memory_data_assets: dict[str, DataAsset] = {}

    @staticmethod
    def _get_in_memory_data_asset_name(
        datasource_name: str, data_asset_name: str
    ) -> str:
        return f"{datasource_name}-{data_asset_name}"

    @override
    @property
    def data(self) -> dict[str, FluentDatasource | BaseDatasource]:  # type: ignore[override] # `data` is meant to be a writeable attr (not a read-only property)
        """
        `data` is referenced by the parent `UserDict` and enables the class to fulfill its various dunder methods
        (__setitem__, __getitem__, etc)

        This is generated just-in-time as the contents of the store may have changed.
        """
        datasources: dict[str, FluentDatasource | BaseDatasource] = {}

        configs = self._datasource_store.get_all()
        for config in configs:
            try:
                if isinstance(config, FluentDatasource):
                    name = config.name
                    datasources[name] = self._init_fluent_datasource(
                        name=name, ds=config
                    )
                else:
                    name = config["name"]
                    datasources[name] = self._init_block_style_datasource(
                        name=name, config=config
                    )
            except gx_exceptions.DatasourceInitializationError as e:
                logger.warning(f"Cannot initialize datasource {name}: {e}")

        return datasources

    @overload
    def set_datasource(self, name: str, ds: T) -> T:
        ...

    @overload
    def set_datasource(self, name: str, ds: BaseDatasource) -> None:
        ...

    def set_datasource(
        self, name: str, ds: FluentDatasource | BaseDatasource
    ) -> FluentDatasource | None:
        config: FluentDatasource | DatasourceConfig
        if isinstance(ds, FluentDatasource):
            config = self._prep_fds_config_for_set(name=name, ds=ds)
        else:
            config = self._prep_legacy_datasource_config_for_set(name=name, ds=ds)

        datasource = self._datasource_store.set(key=None, value=config)
        if isinstance(datasource, FluentDatasource):
            return self._init_fluent_datasource(name=name, ds=datasource)
        return None

    @override
    def __setitem__(self, name: str, ds: FluentDatasource | BaseDatasource) -> None:
        self.set_datasource(name=name, ds=ds)

    def _prep_fds_config_for_set(
        self, name: str, ds: FluentDatasource
    ) -> FluentDatasource:
        if isinstance(ds, SupportsInMemoryDataAssets):
            for asset in ds.assets:
                if asset.type == _IN_MEMORY_DATA_ASSET_TYPE:
                    in_memory_asset_name: str = (
                        DatasourceDict._get_in_memory_data_asset_name(
                            datasource_name=name,
                            data_asset_name=asset.name,
                        )
                    )
                    self._in_memory_data_assets[in_memory_asset_name] = asset
        return ds

    def _prep_legacy_datasource_config_for_set(
        self, name: str, ds: BaseDatasource
    ) -> DatasourceConfig:
        config = ds.config
        # 20230824 - Chetan - Kind of gross but this ensures that we have what we need for instantiate_class_from_config
        # There's probably a better way to do this with Marshmallow but this works
        config["name"] = name
        config["class_name"] = ds.__class__.__name__
        return datasourceConfigSchema.load(config)

    def _get_ds_from_store(self, name: str) -> FluentDatasource | DatasourceConfig:
        try:
            return self._datasource_store.retrieve_by_name(name)
        except ValueError:
            raise KeyError(f"Could not find a datasource named '{name}'")

    @override
    def __delitem__(self, name: str) -> None:
        ds = self._get_ds_from_store(name)
        self._datasource_store.delete(ds)

    @override
    def __getitem__(self, name: str) -> FluentDatasource | BaseDatasource:
        ds = self._get_ds_from_store(name)

        if isinstance(ds, FluentDatasource):
            return self._init_fluent_datasource(name=name, ds=ds)
        return self._init_block_style_datasource(name=name, config=ds)

    def _init_fluent_datasource(
        self, name: str, ds: FluentDatasource
    ) -> FluentDatasource:
        ds._data_context = self._context
        ds._rebuild_asset_data_connectors()
        if isinstance(ds, SupportsInMemoryDataAssets):
            for asset in ds.assets:
                if asset.type == _IN_MEMORY_DATA_ASSET_TYPE:
                    in_memory_asset_name: str = (
                        DatasourceDict._get_in_memory_data_asset_name(
                            datasource_name=name,
                            data_asset_name=asset.name,
                        )
                    )
                    cached_data_asset = self._in_memory_data_assets.get(
                        in_memory_asset_name
                    )
                    if cached_data_asset:
                        asset.dataframe = cached_data_asset.dataframe
                    else:
                        # Asset is loaded into cache here (even without df) to enable loading of df at a later
                        # time when DataframeAsset.build_batch_request(dataframe=df) is called
                        self._in_memory_data_assets[in_memory_asset_name] = asset
        return ds

    # To be removed once block-style is fully removed (deprecated as of v0.17.2)
    def _init_block_style_datasource(
        self, name: str, config: DatasourceConfig
    ) -> BaseDatasource:
        return self._context._init_block_style_datasource(
            datasource_name=name, datasource_config=config
        )


class CacheableDatasourceDict(DatasourceDict):
    """
    Extends the capabilites of the DatasourceDict by placing a caching layer in front of the underlying store.

    Any retrievals will firstly check an in-memory dictionary before requesting from the store. Other CRUD methods will ensure that
    both cache and store are kept in sync.
    """

    def __init__(
        self,
        context: AbstractDataContext,
        datasource_store: DatasourceStore,
    ):
        super().__init__(
            context=context,
            datasource_store=datasource_store,
        )
        self._cache: dict[str, FluentDatasource | BaseDatasource] = {}

    @override
    @property
    def data(self) -> dict[str, FluentDatasource | BaseDatasource]:  # type: ignore[override] # `data` is meant to be a writeable attr (not a read-only property)
        return self._cache

    @override
    def __contains__(self, name: object) -> bool:
        if name in self.data:
            return True
        try:
            # Resort to store only if not in cache
            _ = self._get_ds_from_store(str(name))
            return True
        except KeyError:
            return False

    @overload
    def set_datasource(self, name: str, ds: T) -> T:
        ...

    @overload
    def set_datasource(self, name: str, ds: BaseDatasource) -> None:
        ...

    @override
    def set_datasource(
        self, name: str, ds: FluentDatasource | BaseDatasource
    ) -> FluentDatasource | None:
        self.data[name] = ds

        # FDS do not use stores
        if not isinstance(ds, FluentDatasource):
            return super().set_datasource(name=name, ds=ds)
        return ds

    @override
    def __delitem__(self, name: str) -> None:
        ds = self.data.pop(name, None)

        # FDS do not use stores
        if not isinstance(ds, FluentDatasource):
            super().__delitem__(name)

    @override
    def __getitem__(self, name: str) -> FluentDatasource | BaseDatasource:
        if name in self.data:
            return self.data[name]

        # Upon cache miss, retrieve from store and add to cache
        ds = super().__getitem__(name)
        self.data[name] = ds
        return ds
