# -*- coding: utf-8 -*-

from typing import Any
import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter.sorter import Sorter

logger = logging.getLogger(__name__)


"""
# TODO: <Alex>Let's use the "f-strings" for logging and exceptions raising, wherever possible.  Thanks!</Alex>
# <WILL> TODO : this function originally was intended to handle more sophisticated type checking... is it too much? 
def is_name_in_list(partition_value: str, reference_list: List[str]) -> bool:
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
"""

class CustomListSorter(Sorter):
    r"""
    CustomListSorter
        - The CustomListSorter is able to sort partitions values according to a custom list. Maybe there can be a better name for this...
        - candidates:
            - CustomSorter - too broad
            - ReferenceListSorter - eww
            - ...
    """
    def __init__(self, name: str, **kwargs):
        reference_list: list = kwargs.get("reference_list")
        self._reference_list = reference_list
        super().__init__(name=name, **kwargs)

    def get_partition_key(self, partition: Partition) -> Any:
        partition_definition: dict = partition.definition
        partition_value: Any = partition_definition[self.name]
        # TODO: <WILL> simple membership check for parition_value in reference_list. is this sufficient?
        if partition_value in self.reference_list:
            return self.reference_list.index(partition_value)
        else:
            raise ValueError(f'Source {partition_value} was not found in Reference list.  Try again...')

    @property
    def reference_list(self) -> list:
        return self._reference_list