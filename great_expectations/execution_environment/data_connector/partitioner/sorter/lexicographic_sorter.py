import logging
from typing import Any

from great_expectations.execution_environment.data_connector.partitioner.partition import (
    Partition,
)
from great_expectations.execution_environment.data_connector.partitioner.sorter.sorter import (
    Sorter,
)

logger = logging.getLogger(__name__)


class LexicographicSorter(Sorter):
    def get_partition_key(self, partition: Partition) -> Any:
        partition_definition: dict = partition.definition
        partition_value: Any = partition_definition[self.name]
        return partition_value
