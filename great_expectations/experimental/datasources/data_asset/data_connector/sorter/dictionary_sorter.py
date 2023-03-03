import logging
from typing import Any, List, Optional

# TODO: <Alex>ALEX</Alex>
# import great_expectations.exceptions as gx_exceptions
# TODO: <Alex>ALEX</Alex>
from great_expectations.core.batch import BatchDefinition  # noqa: TCH001
from great_expectations.experimental.datasources.data_asset.data_connector.sorter import (
    Sorter,
)

logger = logging.getLogger(__name__)


class DictionarySorter(Sorter):
    key_reference_list: Optional[List[Any]] = None

    # TODO: <Alex>ALEX</Alex>
    # def __init__(
    #     self,
    #     name: str,
    #     orderby: str = "asc",
    #     order_keys_by: str = "asc",
    #     key_reference_list: Optional[List[Any]] = None,
    # ) -> None:
    #     """Defines sorting behavior for batch definitions based on batch identifiers in nested dictionary form.
    #
    #     Args:
    #         name: the name of the batch identifier key by which to sort the batch definitions.
    #         orderby: one of "asc" (ascending) or "desc" (descending) - the method by which to sort the dictionary
    #             values.
    #         order_keys_by: one of "asc" (ascending) or "desc" (descending) - the method by which to sort the dictionary
    #             keys.
    #         key_reference_list: an ordered list of keys to use for sorting. The list should be provided in the order by
    #             which the keys take precedence (e.g. the list ["year", "month", "day"] will first sort all dictionaries
    #             by "day" value, then by "month" value, and finally by "year" value.
    #
    #     Returns:
    #         None
    #     """
    #     super().__init__(name=name, orderby=orderby)
    #     if order_keys_by is None or order_keys_by == "asc":
    #         reverse_keys = False
    #     elif order_keys_by == "desc":
    #         reverse_keys = True
    #     else:
    #         raise gx_exceptions.SorterError(
    #             f'Illegal key sort order "{order_keys_by}" for attribute "{name}".'
    #         )
    #
    #     # TODO: <Alex>ALEX-USE_PYDANTIC_VALIDATOR</Alex>
    #     self._reverse_keys = reverse_keys
    #     self._key_reference_list = key_reference_list
    #     # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_keys: Optional[List[Any]]
        if self.key_reference_list is None:
            batch_keys = sorted(
                batch_identifiers[self.key].keys(), reverse=self.reverse
            )
        else:
            batch_keys = [
                key
                for key in self.key_reference_list
                if key in batch_identifiers[self.key].keys()
            ]
        batch_values: List[Any] = [
            batch_identifiers[self.key][key] for key in batch_keys
        ]
        return batch_values

    # TODO: <Alex>ALEX</Alex>
    # @property
    # def reverse_keys(self) -> bool:
    #     return self._reverse_keys
    #
    # @property
    # def key_reference_list(self) -> List[Any]:
    #     return self._key_reference_list  # type: ignore[return-value]
    # TODO: <Alex>ALEX</Alex>
