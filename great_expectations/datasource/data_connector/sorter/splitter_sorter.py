from typing import Any, Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.datasource.data_connector.sorter import (
    DateTimeSorter,
    LexicographicSorter,
    NumericSorter,
    Sorter,
)


class SplitterSorter(Sorter):
    SPLITTER_METHOD_TO_SORTER_METHOD_MAPPING: Dict[str, Optional[Sorter]] = {
        "_split_on_year": DateTimeSorter,
        "_split_on_year_and_month": DateTimeSorter,
        "_split_on_year_and_month_and_day": DateTimeSorter,
        "_split_on_date_parts": DateTimeSorter,
        "_split_on_whole_table": LexicographicSorter,
        "_split_on_column_value": LexicographicSorter,
        "_split_on_converted_datetime": LexicographicSorter,
        "_split_on_divided_integer": NumericSorter,
        "_split_on_mod_integer": NumericSorter,
        "_split_on_multi_column_values": LexicographicSorter,
        "_split_on_hashed_column": LexicographicSorter,
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
        self._sorter_method = self.get_sorter_method_from_splitter_method()

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
    def sorter_method(self) -> Sorter:
        return self._sorter_method

    def get_sorter_method_from_splitter_method(self) -> Sorter:
        splitter_method_to_sorter_method_mapping: Dict[
            str, Optional[Sorter]
        ] = self.SPLITTER_METHOD_TO_SORTER_METHOD_MAPPING
        try:
            sorter_method = splitter_method_to_sorter_method_mapping[
                self._splitter_method
            ]
        except KeyError:
            raise ge_exceptions.SorterError(
                f"No Sorter is defined for splitter_method: {self._splitter_method}"
            )
        return sorter_method
