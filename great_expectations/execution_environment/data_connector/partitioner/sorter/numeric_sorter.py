import logging
from typing import Any

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_environment.data_connector.partitioner.partition import (
    Partition,
)
from great_expectations.execution_environment.data_connector.partitioner.sorter.sorter import (
    Sorter,
)
from great_expectations.util import is_int, is_numeric

logger = logging.getLogger(__name__)


class NumericSorter(Sorter):
    def get_partition_key(self, partition: Partition) -> Any:
        partition_definition: dict = partition.definition
        partition_value: Any = partition_definition[self.name]
        if not is_numeric(value=partition_value):
            raise ge_exceptions.SorterError(
                f"""Part "{self.name}" with value "{partition_value}" in partition "{partition.name}" has value
"{partition_value}" which cannot be part of numeric sort.
"""
            )
        if is_int(value=partition_value):
            return int(partition_value)
        # The case of strings having floating point number format used as references to partitions should be rare.
        return round(float(partition_value))
