# -*- coding: utf-8 -*-

import copy
from typing import Union, List, Iterator
from ruamel.yaml.comments import CommentedMap
from great_expectations.data_context.types.base import (
    SorterConfig,
    sorterConfigSchema
)

import logging

from great_expectations.execution_environment.data_connector.partitioner.partition_query import PartitionQuery
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter.sorter import Sorter
from great_expectations.core.id_dict import BatchSpec
import great_expectations.exceptions as ge_exceptions

from great_expectations.data_context.util import (
    instantiate_class_from_config,
)

logger = logging.getLogger(__name__)


class Partitioner(object):
    DEFAULT_DATA_ASSET_NAME: str = "IN_MEMORY_DATA_ASSET"
    DEFAULT_PARTITION_NAME: str = "IN_MEMORY_PARTITION"

    _batch_spec_type: BatchSpec = BatchSpec  #TODO : is this really needed?
    # TODO: <Alex>What makes sense to have here, or is this even needed?</Alex>
    recognized_batch_definition_keys: set = {
        "sorters"
    }

    def __init__(
        self,
        name: str,
        data_connector,
        sorters: list = None,
        allow_multipart_partitions: bool = False,
        config_params: dict = None,
        **kwargs
    ):
        self._name = name
        self._data_connector = data_connector
        self._sorters = sorters
        self._allow_multipart_partitions = allow_multipart_partitions
        self._config_params = config_params
        self._sorters_cache = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_connector(self):
        return self._data_connector

    @property
    def sorters(self) -> Union[List[Sorter], None]:
        if self._sorters:
            return [self.get_sorter(name=sorter_config["name"]) for sorter_config in self._sorters]
        return None

    @property
    def allow_multipart_partitions(self) -> bool:
        return self._allow_multipart_partitions

    @property
    def config_params(self) -> dict:
        return self._config_params

    def get_sorter(self, name) -> Sorter:
        """Get the (named) Sorter from a DataConnector)

        Args:
            name (str): name of Sorter

        Returns:
            Sorter (Sorter)
        """
        if name in self._sorters_cache:
            return self._sorters_cache[name]
        else:
            sorter_names: list = [sorter_config["name"] for sorter_config in self._sorters]
            if name in sorter_names:
                sorter_config: dict = copy.deepcopy(
                    self._sorters[sorter_names.index(name)]
                )
            else:
                raise ge_exceptions.SorterError(
                    f'Unable to load sorter with the name "{name}" -- no configuration found or invalid configuration.'
                )
        sorter_config: CommentedMap = sorterConfigSchema.load(
            sorter_config
        )
        sorter: Sorter = self._build_sorter_from_config(
            name=name, config=sorter_config
        )
        self._sorters_cache[name] = sorter
        return sorter

    @staticmethod
    def _build_sorter_from_config(name: str, config: CommentedMap) -> Sorter:
        """Build a Sorter using the provided configuration and return the newly-built Sorter."""
        # We convert from the type back to a dictionary for purposes of instantiation
        if isinstance(config, SorterConfig):
            config: dict = sorterConfigSchema.dump(config)
        config.update({"name": name})
        sorter: Sorter = instantiate_class_from_config(
            config=config,
            runtime_environment={},
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter"
            },
        )
        if not sorter:
            raise ge_exceptions.ClassInstantiationError(
                module_name="great_expectations.execution_environment.data_connector.partitioner.sorter",
                package_name=None,
                class_name=config["class_name"],
            )
        return sorter

    def get_available_partitions(
        self,
        data_asset_name: str = None,
        partition_query: Union[PartitionQuery, None] = None,
        repartition: bool = False,
        # TODO: <Alex>Accommodating specific partitioner parameters below.</Alex>
        **kwargs
    ) -> List[Partition]:
        if repartition:
            self.data_connector.reset_partitions_cache(data_asset_name=data_asset_name)

        cached_partitions: List[Partition] = self.data_connector.get_cached_partitions(
            data_asset_name=data_asset_name
        )
        if cached_partitions is None or len(cached_partitions) == 0:
            partitions: List[Partition] = self._compute_partitions_for_data_asset(
                data_asset_name=data_asset_name,
                **kwargs
            )
            self.data_connector.update_partitions_cache(
                partitions=partitions,
                allow_multipart_partitions=self.allow_multipart_partitions,
                partitioner=self
            )
            cached_partitions = self.data_connector.get_cached_partitions(
                data_asset_name=data_asset_name
            )
        if cached_partitions is None or len(cached_partitions) == 0:
            return []
        cached_partitions = self.get_sorted_partitions(partitions=cached_partitions)
        if partition_query is None:
            return cached_partitions
        return partition_query.select_partitions(partitions=cached_partitions)

    def get_sorted_partitions(self, partitions: List[Partition]) -> List[Partition]:
        if self.sorters and len(self.sorters) > 0:
            sorters: Iterator[Sorter] = reversed(self.sorters)
            for sorter in sorters:
                partitions = sorter.get_sorted_partitions(partitions=partitions)
            return partitions
        return partitions

    def _compute_partitions_for_data_asset(self, data_asset_name: str = None, **kwargs) -> List[Partition]:
        raise NotImplementedError
