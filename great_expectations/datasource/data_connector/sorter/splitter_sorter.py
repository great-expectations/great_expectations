from typing import Any

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import DateTimeSorter, Sorter


class SplitterSorter(Sorter):
    def __init__(
        self, datetime_sorter: DateTimeSorter, name: str, orderby: str = "asc"
    ) -> None:
        super().__init__(name=name, orderby=orderby)
        self._datetime_sorter = datetime_sorter

    def __repr__(self) -> str:
        doc_fields_dict: dict = {"name": self.name, "reverse": self.reverse}
        return str(doc_fields_dict)

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        partition_value: Any = batch_identifiers[self.name]
        return None
