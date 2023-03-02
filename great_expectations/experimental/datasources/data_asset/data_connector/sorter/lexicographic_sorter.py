import logging
from typing import Any

from great_expectations.core.batch import BatchDefinition  # noqa: TCH001
from great_expectations.experimental.datasources.data_asset.data_connector.sorter import (
    Sorter,
)

logger = logging.getLogger(__name__)


class LexicographicSorter(Sorter):
    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_value: Any = batch_identifiers[self.key]
        return batch_value
