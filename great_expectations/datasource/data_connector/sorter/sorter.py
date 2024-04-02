from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, List, Union, ValuesView

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.typing_extensions import override

if TYPE_CHECKING:
    from great_expectations.core.batch import LegacyBatchDefinition

logger = logging.getLogger(__name__)


class Sorter:
    def __init__(self, name: str, orderby: str = "asc") -> None:
        self._name = name
        if orderby is None or orderby == "asc":
            reverse: bool = False
        elif orderby == "desc":
            reverse = True
        else:
            raise gx_exceptions.SorterError(  # noqa: TRY003
                f'Illegal sort order "{orderby}" for attribute "{name}".'
            )
        self._reverse = reverse

    def get_sorted_batch_definitions(
        self, batch_definitions: List[LegacyBatchDefinition]
    ) -> List[LegacyBatchDefinition]:
        none_batches: List[int] = []
        value_batches: List[int] = []
        for idx, batch_definition in enumerate(batch_definitions):
            # if the batch_identifiers take the form of a nested dictionary, we need to extract the values of the  # noqa: E501
            # inner dict to check for special case sorting of None
            batch_identifier_values: Union[list, ValuesView]
            if len(list(batch_definition.batch_identifiers.values())) == 0:
                batch_identifier_values = [None]
            elif isinstance(list(batch_definition.batch_identifiers.values())[0], dict):
                batch_identifier_values = list(batch_definition.batch_identifiers.values())[
                    0
                ].values()
            else:
                batch_identifier_values = batch_definition.batch_identifiers.values()

            if None in batch_identifier_values or len(batch_identifier_values) == 0:
                none_batches.append(idx)
            else:
                value_batches.append(idx)

        none_batch_definitions: List[LegacyBatchDefinition] = [
            batch_definitions[idx] for idx in none_batches
        ]
        value_batch_definitions: List[LegacyBatchDefinition] = sorted(
            [batch_definitions[idx] for idx in value_batches],
            key=self.get_batch_key,
            reverse=self.reverse,
        )

        # the convention for ORDER BY in SQL is for NULL values to be first in the sort order for ascending  # noqa: E501
        # and last in the sort order for descending
        if self.reverse:
            return value_batch_definitions + none_batch_definitions
        return none_batch_definitions + value_batch_definitions

    def get_batch_key(self, batch_definition: LegacyBatchDefinition) -> Any:
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self._name

    @property
    def reverse(self) -> bool:
        return self._reverse

    @override
    def __repr__(self) -> str:
        doc_fields_dict: dict = {"name": self.name, "reverse": self.reverse}
        return json.dumps(doc_fields_dict, indent=2)
