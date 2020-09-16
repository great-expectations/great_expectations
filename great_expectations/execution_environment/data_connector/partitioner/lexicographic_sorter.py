# -*- coding: utf-8 -*-

from typing import Any
import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter import Sorter

logger = logging.getLogger(__name__)


class LexicographicSorter(Sorter):
    r"""
    LexicographicSorter help
    """
    def get_partition_key(self, partition: Partition) -> Any:
        print(f'[ALEX_DEV:LEXICOGRAPHIC_SORTER#get_partition_key] NAME: {self._name} ; ORDERBY: {self._orderby}')
        partition_definition: dict = partition.definition
        partition_value: Any = partition_definition[self.name]
        print(f'[ALEX_DEV:LEXICOGRAPHIC_SORTER#get_partition_key] PARTITION_DEFINITION: {partition_definition} ; PARTITION_VALUE: {partition_value}')
        # return str.lower(partition_value), partition_value
        return partition_value
