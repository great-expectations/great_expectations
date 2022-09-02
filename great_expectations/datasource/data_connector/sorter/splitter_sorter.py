from typing import Any, Dict

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import (
    CustomListSorter,
    DateTimeSorter,
    LexicographicSorter,
    NumericSorter,
    Sorter,
)


class SplitterSorter(Sorter):
    def __init__(
        self,
        name: str,
        splitter_method: str,
        splitter_kwargs: dict,
        orderby: str = "asc",
    ) -> None:
        super().__init__(name=name, orderby=orderby)
        self._splitter_method = splitter_method
        self._splitter_kwargs = splitter_kwargs

    def __repr__(self) -> str:
        doc_fields_dict: dict = {"name": self.name, "reverse": self.reverse}
        return str(doc_fields_dict)

    @property
    def splitter_method(self) -> str:
        return self._splitter_method

    @property
    def splitter_kwargs(self) -> dict:
        return self._splitter_kwargs

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        value: Any = batch_identifiers[self.name]
        return None
