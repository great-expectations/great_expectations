# TODO: <Alex>ALEX</Alex>
import logging
from typing import Any, List, Union, ValuesView

# TODO: <Alex>ALEX</Alex>
import pydantic

# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
# from pydantic import dataclasses as pydantic_dc
# TODO: <Alex>ALEX</Alex>
from great_expectations.core.batch import BatchDefinition  # noqa: TCH001

# TODO: <Alex>ALEX</Alex>
# from great_expectations.experimental.datasources.interfaces import BatchSorter
# TODO: <Alex>ALEX</Alex>

logger = logging.getLogger(__name__)


# TODO: <Alex>ALEX</Alex>
# class Sorter(BatchSorter):
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
# class Sorter(pydantic.BaseModel):
# TODO: <Alex>ALEX</Alex>
@pydantic.dataclasses.dataclass(frozen=False)
# TODO: <Alex>ALEX</Alex>
class Sorter:
    # TODO: <Alex>ALEX</Alex>
    key: str
    reverse: bool = False
    #
    # TODO: <Alex>ALEX</Alex>
    def get_sorted_batch_definitions(
        self, batch_definitions: List[BatchDefinition]
    ) -> List[BatchDefinition]:
        none_batches: List[int] = []
        value_batches: List[int] = []
        for idx, batch_definition in enumerate(batch_definitions):
            # if the batch_identifiers take the form of a nested dictionary, we need to extract the values of the
            # inner dict to check for special case sorting of None
            batch_identifier_values: Union[list, ValuesView]
            if len(list(batch_definition.batch_identifiers.values())) == 0:
                batch_identifier_values = [None]
            elif isinstance(list(batch_definition.batch_identifiers.values())[0], dict):
                batch_identifier_values = list(
                    batch_definition.batch_identifiers.values()
                )[0].values()
            else:
                batch_identifier_values = batch_definition.batch_identifiers.values()

            if None in batch_identifier_values or len(batch_identifier_values) == 0:
                none_batches.append(idx)
            else:
                value_batches.append(idx)

        none_batch_definitions: List[BatchDefinition] = [
            batch_definitions[idx] for idx in none_batches
        ]
        value_batch_definitions: List[BatchDefinition] = sorted(
            [batch_definitions[idx] for idx in value_batches],
            key=self.get_batch_key,
            reverse=self.reverse,
        )

        # the convention for ORDER BY in SQL is for NULL values to be first in the sort order for ascending
        # and last in the sort order for descending
        if self.reverse:
            return value_batch_definitions + none_batch_definitions

        return none_batch_definitions + value_batch_definitions

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        raise NotImplementedError
