from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.data_connector.sorter import Sorter

if TYPE_CHECKING:
    from great_expectations.core.batch import LegacyBatchDefinition

logger = logging.getLogger(__name__)


class LexicographicSorter(Sorter):
    @override
    def get_batch_key(self, batch_definition: LegacyBatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_value: Any = batch_identifiers[self.name]
        return batch_value

    @override
    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "name": self.name,
            "reverse": self.reverse,
            "type": "LexicographicSorter",
        }
        return json.dumps(doc_fields_dict, indent=2)
