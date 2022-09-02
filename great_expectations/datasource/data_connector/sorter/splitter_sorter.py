from typing import Any, Dict, Optional

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
    SPLITTER_METHOD_TO_SORTER_MAPPING: Dict[str, Optional[Sorter]] = {
        "split_on_year": DateTimeSorter,
        "split_on_year_and_month": DateTimeSorter,
        "split_on_year_and_month_and_day": DateTimeSorter,
        "split_on_date_parts": DateTimeSorter,
        "split_on_whole_table": LexicographicSorter,
        "split_on_column_value": CustomListSorter,
        "split_on_converted_datetime": LexicographicSorter,
        "split_on_divided_integer": NumericSorter,
        "split_on_mod_integer": NumericSorter,
        "split_on_multi_column_values": LexicographicSorter,
        "split_on_hashed_column": LexicographicSorter,
    }

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

    @property
    def sorter(self) -> dict:
        return self._sorter

    def get_sorter_from_splitter_method(self) -> Sorter:
        splitter_method_to_sorter_mapping: Dict[
            str, Optional[Sorter]
        ] = self.SPLITTER_METHOD_TO_SORTER_MAPPING
        try:
            sorter = splitter_method_to_sorter_mapping[self._splitter_method]
        except KeyError:
            raise ge_exceptions.SorterError(
                f"No Sorter is defined for splitter_method: {self._splitter_method}"
            )
        return sorter

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        sorter: Sorter = self.get_sorter_from_splitter_method()
        key: Any = sorter.get_batch_key(batch_definition=batch_definition)
        return key
