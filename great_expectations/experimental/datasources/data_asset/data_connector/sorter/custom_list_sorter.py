# TODO: <Alex>ALEX</Alex>
# import json
# TODO: <Alex>ALEX</Alex>
import logging
from typing import Any, List, Optional

# TODO: <Alex>ALEX</Alex>
# import pydantic
# TODO: <Alex>ALEX</Alex>
import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import BatchDefinition  # noqa: TCH001
from great_expectations.experimental.datasources.data_asset.data_connector.sorter import (
    Sorter,
)

logger = logging.getLogger(__name__)


class CustomListSorter(Sorter):
    """
    CustomListSorter
        - The CustomListSorter is able to sort partitions values according to a user-provided custom list.
    """

    reference_list: Optional[List[str]] = None

    # TODO: <Alex>ALEX</Alex>
    # @pydantic.validator("reference_list", pre=True)
    # # TODO: <Alex>ALEX</Alex>
    # # @staticmethod
    # # TODO: <Alex>ALEX</Alex>
    # def _validate_reference_list(cls, reference_list: Optional[List[str]] = None) -> List[str]:
    #     if not (reference_list and isinstance(reference_list, list)):
    #         raise gx_exceptions.SorterError(
    #             "CustomListSorter requires reference_list which was not provided."
    #         )
    #
    #     for item in reference_list:
    #         if not isinstance(item, str):
    #             raise gx_exceptions.SorterError(
    #                 f"Items in reference list for CustomListSorter must have string type (actual type is `{str(type(item))}`)."
    #             )
    #
    #     return reference_list
    # TODO: <Alex>ALEX</Alex>

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_value: Any = batch_identifiers[self.key]
        if batch_value in self.reference_list:
            return self.reference_list.index(batch_value)

        raise gx_exceptions.SorterError(
            f"Source {batch_value} was not found in Reference list.  Try again..."
        )

    # TODO: <Alex>ALEX</Alex>
    # def __repr__(self) -> str:
    #     doc_fields_dict: dict = {
    #         "name": self.key,
    #         "reverse": self.reverse,
    #         "type": "CustomListSorter",
    #     }
    #     return json.dumps(doc_fields_dict, indent=2)
    # TODO: <Alex>ALEX</Alex>
