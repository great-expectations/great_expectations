from typing import Any, Dict, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import (
    DateTimeSorter,
    LexicographicSorter,
    NumericSorter,
    Sorter,
)
from great_expectations.execution_engine.split_and_sample.data_splitter import (
    DataSplitter,
)


class SplitterSorter(Sorter):
    SPLITTER_METHOD_TO_SORTER_METHOD_MAPPING: Dict[str, Optional[Sorter]] = {
        "split_on_year": DateTimeSorter,
        "split_on_year_and_month": DateTimeSorter,
        "split_on_year_and_month_and_day": DateTimeSorter,
        "split_on_date_parts": DateTimeSorter,
        "split_on_whole_table": LexicographicSorter,
        "split_on_column_value": LexicographicSorter,
        "split_on_converted_datetime": LexicographicSorter,
        "split_on_divided_integer": NumericSorter,
        "split_on_mod_integer": NumericSorter,
        "split_on_multi_column_values": LexicographicSorter,
        "split_on_hashed_column": LexicographicSorter,
    }

    def __init__(
        self,
        name: str,
        splitter_method_name: str,
        splitter_kwargs: dict,
        orderby: str = "asc",
    ) -> None:
        super().__init__(name=name, orderby=orderby)
        self._splitter_method_name = splitter_method_name
        self._splitter_kwargs = splitter_kwargs
        self._sorter_method = self.get_sorter_method_from_splitter_method()

    def __repr__(self) -> str:
        doc_fields_dict: dict = {"name": self.name, "reverse": self.reverse}
        return str(doc_fields_dict)

    @property
    def splitter_method_name(self) -> str:
        return self._splitter_method_name

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
        splitter_method_name: str = self._get_splitter_method_name(
            splitter_method_name=self.splitter_method_name
        )
        try:
            sorter_method = splitter_method_to_sorter_method_mapping[
                splitter_method_name
            ]
        except KeyError:
            raise ge_exceptions.SorterError(
                f"No Sorter is defined for splitter_method: {splitter_method_name}"
            )
        return sorter_method

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_value: Any = batch_identifiers[self.name]
        return batch_value

    @staticmethod
    def _get_splitter_method_name(splitter_method_name: str) -> str:
        """Accept splitter methods with or without starting with `_`.

        Args:
            splitter_method_name: splitter name starting with or without preceding `_`.

        Returns:
            splitter method name stripped of preceding underscore.
        """
        if splitter_method_name.startswith("_"):
            return splitter_method_name[1:]
        else:
            return splitter_method_name
