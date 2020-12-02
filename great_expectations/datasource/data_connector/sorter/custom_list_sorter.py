import logging
from typing import Any, List

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import Sorter

logger = logging.getLogger(__name__)


class CustomListSorter(Sorter):
    """
    CustomListSorter
        - The CustomListSorter is able to sort partitions values according to a user-provided custom list.
    """

    def __init__(
        self, name: str, orderby: str = "asc", reference_list: List[str] = None
    ):
        super().__init__(name=name, orderby=orderby)

        self._reference_list = self._validate_reference_list(
            reference_list=reference_list
        )

    @staticmethod
    def _validate_reference_list(reference_list: List[str] = None) -> List[str]:
        if not (reference_list and isinstance(reference_list, list)):
            raise ge_exceptions.SorterError(
                "CustomListSorter requires reference_list which was not provided."
            )
        for item in reference_list:
            if not isinstance(item, str):
                raise ge_exceptions.SorterError(
                    f"Items in reference list for CustomListSorter must have string type (actual type is `{str(type(item))}`)."
                )
        return reference_list

    def get_partition_key(self, batch_definition: BatchDefinition) -> Any:
        partition_definition: dict = batch_definition.partition_definition
        partition_value: Any = partition_definition[self.name]
        if partition_value in self._reference_list:
            return self._reference_list.index(partition_value)
        else:
            raise ge_exceptions.SorterError(
                f"Source {partition_value} was not found in Reference list.  Try again..."
            )

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "name": self.name,
            "reverse": self.reverse,
            "type": "CustomListSorter",
        }
        return str(doc_fields_dict)
