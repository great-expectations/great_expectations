import logging
from typing import Any

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import Sorter
from great_expectations.util import is_int, is_numeric

logger = logging.getLogger(__name__)


class NumericSorter(Sorter):
    def get_partition_key(self, batch_definition: BatchDefinition) -> Any:
        partition_definition: dict = batch_definition.partition_definition
        partition_value: Any = partition_definition[self.name]
        if not is_numeric(value=partition_value):
            raise ge_exceptions.SorterError(
                # what is the identifying characteristic of batch_definition?
                f"""BatchDefinition with PartitionDefinition "{self.name}" with value "{partition_value}" has value
"{partition_value}" which cannot be part of numeric sort.
"""
            )
        if is_int(value=partition_value):
            return int(partition_value)
        # The case of strings having floating point number format used as references to partitions should be rare.
        return round(float(partition_value))

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "name": self.name,
            "reverse": self.reverse,
            "type": "NumericSorter",
        }
        return str(doc_fields_dict)
