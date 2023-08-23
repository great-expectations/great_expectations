from __future__ import annotations

import copy
import logging
from collections import UserDict
from typing import TYPE_CHECKING

import great_expectations.exceptions as gx_exceptions
from great_expectations.data_context.types.base import (
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
        return {key.resource_name for key in keys}

    @property
    def data(self) -> dict[str, FluentDatasource | BaseDatasource]:
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

    def __contains__(self, name: str) -> bool:
        return name in self._names

    def __setitem__(self, _: str, ds: FluentDatasource | BaseDatasource) -> None:
        if isinstance(ds, BaseDatasource):
            config = datasourceConfigSchema.load(ds.config)
        else:
            config = ds

        self._datasource_store.set(key=None, value=config)

    def __getitem__(self, name: str) -> FluentDatasource | BaseDatasource:
        ds = self._datasource_store.retrieve_by_name(name)
        if isinstance(ds, FluentDatasource):
            return self._init_fluent_datasource(ds)
        return self._init_block_datasource(name=name, config=ds)

    def _init_fluent_datasource(self, ds: FluentDatasource) -> FluentDatasource:
        ds._rebuild_asset_data_connectors()
        return ds

    def _init_block_datasource(self, name: str, config: dict) -> BaseDatasource:
        # To be removed once block-style is fully removed
        # Deprecated as of v0.17.2
        config = copy.deepcopy(config)

        raw_config_dict = dict(datasourceConfigSchema.dump(config))
        substituted_config_dict: dict = self._config_provider.substitute_config(
            raw_config_dict
        )

        raw_datasource_config = datasourceConfigSchema.load(raw_config_dict)
        substituted_datasource_config = datasourceConfigSchema.load(
            substituted_config_dict
        )
        substituted_datasource_config.name = name

        return self._context._instantiate_datasource_from_config(
            raw_config=raw_datasource_config,
            substituted_config=substituted_datasource_config,
        )
