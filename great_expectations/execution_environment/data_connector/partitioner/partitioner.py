# -*- coding: utf-8 -*-

import logging
import copy
from typing import List
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

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_connector(self) -> DataConnector:
        return self._data_connector

    @property
    def config_params(self) -> dict:
        return self._partitioner_config.get("config_params")

    def get_available_partitions(self, partition_name: str = None, data_asset_name: str = None) -> List[Partition]:
        raise NotImplementedError
    #<ALEX> Please erase
    #def get_available_partition_names(self, data_asset_name: str = None) -> List[str]:
    #    return [
    #        partition.name for partition in self.get_available_partitions(
    #            partition_name=None,
    #            data_asset_name=data_asset_name
    #        )
    #    ]

    def _build_sorter(self, **kwargs):
        """Build a Sorter using pattern that uses instate_class_from_config() function"""
        sorter = instantiate_class_from_config(
            config=kwargs,
            runtime_environment={"partitioner": self},
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter" # <TODO><WILL> confirm if this is the final spot?
            }
        )

        if not sorter:
            raise ClassInstantiationError(
                module_name="great_expectations.execution_environment.data_connector.partitioner.sorter",
                package_name=None,
                class_name=kwargs["class_name"],
            )
        return sorter

    def get_sorter(self, name):
        if name in self._sorters:
            return self._sorters[name]
        elif(
            "sorters" in self._partitioner_config
            and name in self._partitioner_config["sorters"]
        ):
            sorter_config = copy.deepcopy(
                self._partitioner_config["sorters"][name]
            )
        else:
            raise ValueError(
                "Unable to load Sorter %s for Partitioner %s -- no configuration found or invalid configuration."
                % name, self._name
            )
        sorter_config["name"] = name
        sorter = self._build_sorter(**sorter_config)
        self._sorters[name] = sorter
        return sorter

    def get_all_sorters(self):
        sorters = [
            self.get_sorter(name)
            for name in self._partitioner_config["sorters"]
            if self.get_sorter(name) is not None
        ]
        return sorters
