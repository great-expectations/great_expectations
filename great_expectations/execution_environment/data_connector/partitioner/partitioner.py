# -*- coding: utf-8 -*-

import logging
import copy
from typing import List
from great_expectations.data_context.types.base import (
    SorterConfig,
    sorterConfigSchema
)
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.id_dict import BatchSpec

from great_expectations.exceptions import ClassInstantiationError

from great_expectations.data_context.util import (
    instantiate_class_from_config,
)

logger = logging.getLogger(__name__)


class Partitioner(object):
    r"""
    Partitioners help
    """

    _batch_spec_type = BatchSpec  #TODO : is this really needed?
    recognized_batch_definition_keys = {
        "regex",
        "sorters"
    }

    def __init__(self, name: str, data_connector: DataConnector, **kwargs):
        self._name = name
        self._data_connector = data_connector
        # TODO: <Alex></Alex>
        self._partitioner_config = kwargs
        self._sorters_cache = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_connector(self) -> DataConnector:
        return self._data_connector

    @property
    def config_params(self) -> dict:
        return self._partitioner_config.get("config_params")

    # TODO: <Alex>Add typehints throughout.</Alex>
    @property
    def sorters(self):
        sorter_config_list = self._partitioner_config.get("sorters")
        if sorter_config_list:
            sorters = [
                self.get_sorter(name=sorter_config["name"])
                for sorter_config in sorter_config_list
                if sorter_config is not None
            ]
            return sorters
        return None

    # TODO: <Alex>Add typehints throughout</Alex>
    def get_sorter(self, name):
        """Get the (named) Sorter from a DataConnector)

        Args:
            name (str): name of Sorter

        Returns:
            Sorter (Sorter)
        """
        # TODO: <Alex>This could be made more efficient, but numbers of sorters is small and this pattern is useful.</Alex>
        if name in self._sorters_cache:
            return self._sorters_cache[name]
        elif (
            "sorters" in self._partitioner_config
            and name in [sorter_config["name"] for sorter_config in self._partitioner_config["sorters"]]
        ):
            sorter_config_list = self._partitioner_config["sorters"]
            sorter_names = [sorter_config["name"] for sorter_config in sorter_config_list]
            sorter_config = copy.deepcopy(
                sorter_config_list[sorter_names.index(name)]
            )
        else:
            raise ValueError(
                'Unable to load sorter with the name "%s" -- no configuration found or invalid configuration.'
                % name
            )
        sorter_config = sorterConfigSchema.load(
            sorter_config
        )
        sorter = self._build_sorter_from_config(
            name=name, config=sorter_config
        )
        self._sorters_cache[name] = sorter
        return sorter

    # TODO: <Alex>This is a good place to check that all defaults from base.py / Config Schemas are set properly.</Alex>
    @staticmethod
    def _build_sorter_from_config(name, config):
        """Build a Sorter using the provided configuration and return the newly-built Sorter."""
        # We convert from the type back to a dictionary for purposes of instantiation
        if isinstance(config, SorterConfig):
            config = sorterConfigSchema.dump(config)
        config.update({"name": name})
        sorter = instantiate_class_from_config(
            config=config,
            runtime_environment={},
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector.partitioer.sorter"
            },
        )
        if not sorter:
            raise ClassInstantiationError(
                module_name="great_expectations.execution_environment.data_connector.partitioer.sorter",
                package_name=None,
                class_name=config["class_name"],
            )
        return sorter

    # def get_sorter(self, name):
    #     if name in self._sorters:
    #         return self._sorters[name]
    #     elif(
    #         "sorters" in self._partitioner_config
    #         and name in self._partitioner_config["sorters"]
    #     ):
    #         sorter_config = copy.deepcopy(
    #             self._partitioner_config["sorters"][name]
    #         )
    #     else:
    #         raise ValueError(
    #             "Unable to load Sorter %s for Partitioner %s -- no configuration found or invalid configuration."
    #             % name, self._name
    #         )
    #     sorter_config["name"] = name
    #     sorter = self._build_sorter(**sorter_config)
    #     self._sorters[name] = sorter
    #     return sorter
    #
    # def _build_sorter(self, **kwargs):
    #     """Build a Sorter using pattern that uses instate_class_from_config() function"""
    #     sorter = instantiate_class_from_config(
    #         config=kwargs,
    #         runtime_environment={"partitioner": self},
    #         config_defaults={
    #             "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter" # <TODO><WILL> confirm if this is the final spot?
    #         }
    #     )
    #
    #     if not sorter:
    #         raise ClassInstantiationError(
    #             module_name="great_expectations.execution_environment.data_connector.partitioner.sorter",
    #             package_name=None,
    #             class_name=kwargs["class_name"],
    #         )
    #     return sorter

    def get_available_partitions(self, partition_name: str = None, data_asset_name: str = None) -> List[Partition]:
        raise NotImplementedError
