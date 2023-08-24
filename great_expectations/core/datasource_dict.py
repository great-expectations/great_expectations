from __future__ import annotations

import logging
from collections import UserDict
from typing import TYPE_CHECKING

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource.fluent import Datasource as FluentDatasource

if TYPE_CHECKING:
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.data_context.store.datasource_store import DatasourceStore
    from great_expectations.datasource.new_datasource import BaseDatasource

logger = logging.getLogger(__name__)


class DatasourceDict(UserDict):
    """
    An abstraction around the DatasourceStore to enable easy retrieval and storage of Datasource objects
    using dictionary syntactic sugar.
    """

    def __init__(
        self,
        context: AbstractDataContext,
        datasource_store: DatasourceStore,
        config_provider: _ConfigurationProvider,
    ):
        self._context = context  # If possible, we should avoid passing the context through - once block-style is removed, we can extract this
        self._datasource_store = datasource_store
        self._config_provider = config_provider

    @property
    def _names(self) -> set[str]:
        # The contents of the store may change between uses so we constantly refresh when requested
        keys = self._datasource_store.list_keys()
        return {key.resource_name for key in keys}  # type: ignore[attr-defined] # list_keys() is annotated with generic DataContextKey instead of subclass

    @override
    @property
    def data(self) -> dict[str, FluentDatasource | BaseDatasource]:  # type: ignore[override] # `data` is meant to be a writeable attr (not a read-only property)
        """
        `data` is referenced by the parent `UserDict` and enables the class to fulfill its various dunder methods
        (__contains__, __setitem__, __getitem__, etc)

        While simply fulfilling this property would be sufficient to get the `DatasourceDict` to work, it is an
        expensive operation (requires requesting ALL datasources and constructing them in memory).

        As a reasonable middle ground, we override certain dunder methods to be more performant (ex: see __contains__)
        """
        datasources = {}
        for name in self._names:
            try:
                datasources[name] = self.__getitem__(name)
            except gx_exceptions.DatasourceInitializationError as e:
                logger.warning(f"Cannot initialize datasource {name}: {e}")

        return datasources

    @override
    def __contains__(self, name: object) -> bool:
        return name in self._names

    @override
    def __setitem__(self, name: str, ds: FluentDatasource | BaseDatasource) -> None:
        if isinstance(ds, FluentDatasource):
            config = ds
        else:
            config = datasourceConfigSchema.load(ds.config)

        self._datasource_store.set(key=None, value=config)

    @override
    def __delitem__(self, name: str) -> None:
        if not self.__contains__(name):
            raise KeyError(f"Could not find a datasource named '{name}'")

        ds = self._datasource_store.retrieve_by_name(name)
        self._datasource_store.delete(ds)

    @override
    def __getitem__(self, name: str) -> FluentDatasource | BaseDatasource:
        if not self.__contains__(name):
            raise KeyError(f"Could not find a datasource named '{name}'")

        ds = self._datasource_store.retrieve_by_name(name)
        if isinstance(ds, FluentDatasource):
            return self._init_fluent_datasource(ds)
        return self._init_block_style_datasource(name=name, config=ds)

    def _init_fluent_datasource(self, ds: FluentDatasource) -> FluentDatasource:
        ds._rebuild_asset_data_connectors()
        return ds

    # To be removed once block-style is fully removed (deprecated as of v0.17.2)
    def _init_block_style_datasource(
        self, name: str, config: DatasourceConfig
    ) -> BaseDatasource:
        return self._context._init_block_style_datasource(
            datasource_name=name, datasource_config=config
        )


class CacheEnabledDatasourceDict(DatasourceDict):
    """
    Extends the capabilites of the DatasourceDict by placing a caching layer in front of the underlying store.

    Any retrievals will firstly check an in-memory dictionary before requesting from the store. Other CRUD methods will ensure that
    both cache and store are kept in sync.
    """

    def __init__(
        self,
        context: AbstractDataContext,
        datasource_store: DatasourceStore,
        config_provider: _ConfigurationProvider,
    ):
        super().__init__(
            context=context,
            datasource_store=datasource_store,
            config_provider=config_provider,
        )
        self._cache: dict[str, FluentDatasource | BaseDatasource] = {}

    @override
    @property
    def data(self) -> dict[str, FluentDatasource | BaseDatasource]:  # type: ignore[override] # `data` is meant to be a writeable attr (not a read-only property)
        return self._cache

    @override
    def __contains__(self, name: object) -> bool:
        return name in self.data or super().__contains__(name)

    @override
    def __setitem__(self, name: str, ds: FluentDatasource | BaseDatasource) -> None:
        self.data[name] = ds
        super().__setitem__(name, ds)

    @override
    def __delitem__(self, name: str) -> None:
        self.data.pop(name, None)
        super().__delitem__(name)

    @override
    def __getitem__(self, name: str) -> FluentDatasource | BaseDatasource:
        if name in self.data:
            return self.data[name]

        # Upon cache miss, retrieve from store and add to cache
        ds = super().__getitem__(name)
        self.data[name] = ds
        return ds
