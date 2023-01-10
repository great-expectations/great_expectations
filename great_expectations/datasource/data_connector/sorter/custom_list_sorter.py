import json
import logging
from typing import Any, List, Optional

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import Sorter

logger = logging.getLogger(__name__)


class CustomListSorter(Sorter):
    """
    CustomListSorter
        - The CustomListSorter is able to sort partitions values according to a user-provided custom list.
    """

    def __init__(
        self,
        name: str,
        orderby: str = "asc",
        reference_list: Optional[List[str]] = None,
    ) -> None:
        super().__init__(name=name, orderby=orderby)

        self._reference_list = self._validate_reference_list(
            reference_list=reference_list
        )

    @staticmethod
    def _validate_reference_list(
        reference_list: Optional[List[str]] = None,
    ) -> List[str]:
        if not (reference_list and isinstance(reference_list, list)):
            raise gx_exceptions.SorterError(
                "CustomListSorter requires reference_list which was not provided."
            )
        for item in reference_list:
            if not isinstance(item, str):
                raise gx_exceptions.SorterError(
                    f"Items in reference list for CustomListSorter must have string type (actual type is `{str(type(item))}`)."
                )
        return reference_list

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_value: Any = batch_identifiers[self.name]
        if batch_value in self._reference_list:
            return self._reference_list.index(batch_value)
        else:
            raise gx_exceptions.SorterError(
                f"Source {batch_value} was not found in Reference list.  Try again..."
            )

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "name": self.name,
            "reverse": self.reverse,
            "type": "CustomListSorter",
        }
        return json.dumps(doc_fields_dict, indent=2)
