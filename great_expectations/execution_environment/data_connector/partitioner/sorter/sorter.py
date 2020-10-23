# -*- coding: utf-8 -*-

from typing import List, Any

import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


class Sorter(object):
    def __init__(self, name: str, orderby: str = "asc"):
        self._name = name
        if orderby is None or orderby == "asc":
            reverse: bool = False
        elif orderby == "desc":
            reverse: bool = True
        else:
            raise ge_exceptions.SorterError(f'Illegal sort order "{orderby}" for attribute "{name}".')
        self._reverse = reverse

    def get_sorted_partitions(self, partitions: List[Partition]) -> List[Partition]:
        return sorted(partitions, key=self._verify_sorting_directives_and_get_partition_key, reverse=self.reverse)

    def _verify_sorting_directives_and_get_partition_key(self, partition: Partition) -> Any:
        partition_definition: dict = partition.definition
        if partition_definition.get(self.name) is None:
            raise ge_exceptions.SorterError(f'Unable to sort partition "{partition.name}" by attribute "{self.name}".')
        return self.get_partition_key(partition=partition)

    def get_partition_key(self, partition: Partition) -> Any:
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self._name

    @property
    def reverse(self) -> bool:
        return self._reverse

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "name": self.name,
            "reverse": self.reverse
        }
        return str(doc_fields_dict)
