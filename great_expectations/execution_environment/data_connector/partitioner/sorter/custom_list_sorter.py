# -*- coding: utf-8 -*-

from typing import Any, List

import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter.sorter import Sorter
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


class CustomListSorter(Sorter):

    """
    CustomListSorter
        - The CustomListSorter is able to sort partitions values according to a custom list. Maybe there can be a better name for this...
    """

    def __init__(self, name: str, orderby: str = "asc", config_params: dict = None, **kwargs):
        super().__init__(name=name, orderby=orderby, config_params=config_params, **kwargs)
        reference_list: list = self.config_params.get("reference_list")
        self._reference_list = self._validate_reference_list(reference_list=reference_list)

    @staticmethod
    def _validate_reference_list(reference_list: List[str] = None) -> List[str]:
        for item in reference_list:
            if not isinstance(item, str):
                raise ge_exceptions.SorterError(
                    f"Items in reference list for CustomListSorter must have string type (actual type is {str(type(item))})"
                )
        return reference_list

    def get_partition_key(self, partition: Partition) -> Any:
        partition_definition: dict = partition.definition
        partition_value: Any = partition_definition[self.name]
        if partition_value in self.reference_list:
            return self.reference_list.index(partition_value)
        else:
            raise ge_exceptions.SorterError(f'Source {partition_value} was not found in Reference list.  Try again...')

    @property
    def reference_list(self) -> list:
        return self._reference_list
