# -*- coding: utf-8 -*-

from typing import Any

import logging

#from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.batch import BatchDefinition

from great_expectations.execution_environment.data_connector.sorter.sorter import (
Sorter,
)

logger = logging.getLogger(__name__)


class LexicographicSorter(Sorter):
    def get_partition_key(self, batch_definition: BatchDefinition) -> Any:
        partition_definition: dict = batch_definition.partition_definition
        partition_value: Any = partition_definition[self.name]
        return partition_value
