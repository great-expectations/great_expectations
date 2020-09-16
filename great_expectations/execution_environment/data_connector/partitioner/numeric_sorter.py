# -*- coding: utf-8 -*-

from typing import Any
import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter import Sorter

logger = logging.getLogger(__name__)


def is_numeric(value: Any) -> bool:
    return is_int(value) or is_float(value)


def is_int(value: Any) -> bool:
    try:
        num: int = int(value)
    except ValueError:
        return False
    return True


def is_float(value: Any) -> bool:
    try:
        num: float = float(value)
    except ValueError:
        return False
    return True


class NumericSorter(Sorter):
    r"""
    NumericSorter help
    """
    def get_partition_key(self, partition: Partition) -> Any:
        partition_definition: dict = partition.definition
        partition_value: Any = partition_definition[self.name]
        if not is_numeric(value=partition_value):
            raise ValueError(
                f'''Part "{self.name}" with value "{partition_value}" in partition "{partition.name}" has value
"{partition_value}" which cannot be part of numeric sort.
'''
            )
        if is_int(value=partition_value):
            return int(partition_value)
        return round(float(partition_value))
