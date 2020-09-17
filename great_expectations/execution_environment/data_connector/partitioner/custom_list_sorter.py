# -*- coding: utf-8 -*-

from typing import Any, List
import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter import Sorter

logger = logging.getLogger(__name__)



def name_in_list(partition_value: str, reference_list: List[str]) -> bool:
    # check type
    if not isinstance(partition_value, str):
        raise ValueError(
            'Source "partition_value" must have string type (actual type is "{}").'.format(
                str(type(partition_value))
            )
        )

    try:
        reference_list.index(partition_value)
        return True
    except ValueError:
        print('Source ' + partition_value + ' was not found in Reference list.  Try again...')
        return False


class CustomListSorter(Sorter):
    r"""
    CustomListSorter
        - This is a placeholder Sorter for being able to handle a custom list

    """
    def __init__(self, name: str, **kwargs):
        reference_list: list = kwargs.get("reference_list")
        self._reference_list = reference_list
        super().__init__(name=name, **kwargs)

    def get_partition_key(self, partition: Partition) -> Any:
        partition_definition: dict = partition.definition
        partition_value: Any = partition_definition[self.name]
        if name_in_list(partition_value, self.reference_list):
            return self.reference_list.index(partition_value)
        else:
            raise ValueError(
                'Source "partition_value" was not found in Reference list.  Try again...'
            )

    @property
    def reference_list(self) -> list:
        return self._reference_list