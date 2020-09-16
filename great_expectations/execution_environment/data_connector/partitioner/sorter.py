# -*- coding: utf-8 -*-

from typing import Iterable, Any

import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition

logger = logging.getLogger(__name__)


class Sorter(object):
    r"""
    Sorter help
    """

    def __init__(self, name: str, **kwargs):
        self._name = name
        # TODO: <Alex>We need to make sure that this is consistent with default "orderby" value from SorterConfig</Alex>
        orderby: str = kwargs.get("orderby")
        if orderby == "asc":
            reverse = False
        elif orderby == "desc":
            reverse = True
        elif orderby is None:
            reverse = False
        else:
            raise ValueError(f'Illegal sort order "{orderby}" for attribute "{name}".')
        if orderby:
            self._orderby = orderby
        self._reverse = reverse

    def get_sorted_partitions(self, partitions: Iterable[Partition]) -> Iterable[Partition]:
        return sorted(partitions, key=self._verify_sorting_directives_and_get_partition_key, reverse=self.reverse)

    def _verify_sorting_directives_and_get_partition_key(self, partition: Partition) -> Any:
        partition_definition = partition.definition
        if partition_definition.get(self.name) is None:
            raise ValueError(f'Unable to sort partition "{partition.name}" by attribute "{self.name}".')
        return self.get_partition_key(partition=partition)

    def get_partition_key(self, partition: Partition) -> Any:
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self._name

    @property
    def orderby(self) -> str:
        return self._orderby

    @property
    def reverse(self) -> bool:
        return self._reverse
