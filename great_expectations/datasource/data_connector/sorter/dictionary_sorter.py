import json
import logging
from typing import Any, List, Optional

from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import Sorter

logger = logging.getLogger(__name__)


class DictionarySorter(Sorter):
    def __init__(
        self,
        name: str,
        orderby: str = "asc",
        key_reference_list: Optional[List[Any]] = None,
    ) -> None:
        super().__init__(name=name, orderby=orderby)
        self._key_reference_list = key_reference_list

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_keys: Optional[List[Any]]
        if self._key_reference_list is None:
            batch_keys = sorted(batch_identifiers[self.name].keys())
        else:
            batch_keys = self._key_reference_list
        batch_values: List[Any] = [
            batch_identifiers[self.name][key] for key in batch_keys
        ]
        return batch_values

    def __repr__(self) -> str:
        doc_fields_dict = {
            "name": self.name,
            "reverse": self.reverse,
            "type": "DictionarySorter",
        }
        return json.dumps(doc_fields_dict, indent=2)
