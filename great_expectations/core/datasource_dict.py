from __future__ import annotations

import copy
from collections import UserDict
from typing import TYPE_CHECKING, Any

from great_expectations.data_context.types.base import datasourceConfigSchema
from great_expectations.datasource.new_datasource import BaseDatasource

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.datasource.fluent import Datasource as FluentDatasource


class DatasourceDict(UserDict):
    def __init__(self, context: AbstractDataContext):
        self._context = context
        self._datasource_store = context._datasource_store
        self._config_provider = context.config_provider
        self._fluent_config = context.fluent_config

    @property
    def data(self) -> dict[str, FluentDatasource | BaseDatasource]:
        keys = self._datasource_store.list_keys()
        names = [key.resource_name for key in keys]
        return {name: self.__getitem__(name) for name in names}

    def __setitem__(self, _: str, ds: FluentDatasource | BaseDatasource) -> None:
        if isinstance(ds, BaseDatasource):
            config = datasourceConfigSchema.load(ds.config)
        else:
            config = ds

        self._datasource_store.set(key=None, value=config)

    def __getitem__(self, name: str) -> Any:
        config = self._datasource_store.retrieve_by_name(name)
        if "type" in config:
            return self._init_fluent_datasource(config)
        return self._init_block_datasource(name=name, config=config)

    def _init_fluent_datasource(self, config: dict) -> FluentDatasource:
        datasource = self._add_fluent_datasource(**config)
        datasource._rebuild_asset_data_connectors()
        return datasource

    def _init_block_datasource(self, name: str, config: dict) -> BaseDatasource:
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

    def __contains__(self, name: str) -> bool:
        try:
            _ = self.__getitem__(name)
            return True
        except ValueError:
            return False
