from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.data_connector.sorter import Sorter
from great_expectations.util import is_int, is_numeric

if TYPE_CHECKING:
    from great_expectations.core.batch import LegacyBatchDefinition

logger = logging.getLogger(__name__)


class NumericSorter(Sorter):
    @override
    def get_batch_key(self, batch_definition: LegacyBatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_value: Any = batch_identifiers[self.name]
        if not is_numeric(value=batch_value):
            raise gx_exceptions.SorterError(  # noqa: TRY003
                # what is the identifying characteristic of batch_definition?
                f"""BatchDefinition with IDDict "{self.name}" with value "{batch_value}" has value
"{batch_value}" which cannot be part of numeric sort.
"""
            )
        if is_int(value=batch_value):
            return int(batch_value)
        # The case of strings having floating point number format used as references to partitions should be rare.  # noqa: E501
        return round(float(batch_value))

    @override
    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "name": self.name,
            "reverse": self.reverse,
            "type": "NumericSorter",
        }
        return json.dumps(doc_fields_dict, indent=2)
