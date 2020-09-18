# -*- coding: utf-8 -*-

import logging

from typing import List
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.id_dict import BatchSpec

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

    def __init__(self, name: str, data_connector: DataConnector):
        self._name = name
        self._data_connector = data_connector

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_connector(self) -> DataConnector:
        return self._data_connector

    def get_available_partitions(self, partition_name: str = None, data_asset_name: str = None) -> List[Partition]:
        raise NotImplementedError

    def get_available_partition_names(self, data_asset_name: str = None) -> List[str]:
        return [
            partition.name for partition in self.get_available_partitions(
                partition_name=None,
                data_asset_name=data_asset_name
            )
        ]
