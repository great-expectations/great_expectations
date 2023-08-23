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
from great_expectations.datasource.new_datasource import BaseDatasource

if TYPE_CHECKING:
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.data_context.store.datasource_store import DatasourceStore
    from great_expectations.datasource.fluent import Datasource as FluentDatasource

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
    def __setitem__(self, _: str, ds: FluentDatasource | BaseDatasource) -> None:
        if isinstance(ds, BaseDatasource):
            config = datasourceConfigSchema.load(ds.config)
        else:
            config = ds

        self._datasource_store.set(key=None, value=config)

    @override
    def __getitem__(self, name: str) -> FluentDatasource | BaseDatasource:
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
