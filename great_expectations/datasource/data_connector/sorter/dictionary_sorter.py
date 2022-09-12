import json
import logging
from typing import Any, List

from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import Sorter

logger = logging.getLogger(__name__)


class DictionarySorter(Sorter):
    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_values: List[Any] = list(batch_identifiers[self.name].values())
        return batch_values

    def __repr__(self) -> str:
        doc_fields_dict = {
            "name": self.name,
            "reverse": self.reverse,
            "type": "DictionarySorter",
        }
        return json.dumps(doc_fields_dict, indent=2)
