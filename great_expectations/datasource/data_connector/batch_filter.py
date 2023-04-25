from __future__ import annotations

import itertools
import logging
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Union

from typing_extensions import TypeAlias

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.id_dict import IDDict
from great_expectations.core.util import parse_batch_slice

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition

logger = logging.getLogger(__name__)


BatchSlice: TypeAlias = Union[slice, str, List[int], Tuple[int, ...], int]


def build_batch_filter(
    data_connector_query_dict: Optional[
        Dict[
            str,
            Optional[
                Union[
                    int,
                    list,
                    tuple,
                    slice,
                    str,
                    Union[Dict, IDDict],
                    Callable,
                ]
            ],
        ]
    ] = None
):
    if not data_connector_query_dict:
        return BatchFilter(
            custom_filter_function=None,
            batch_filter_parameters=None,
            index=None,
            limit=None,
        )
    data_connector_query_keys: set = set(data_connector_query_dict.keys())
    if not data_connector_query_keys <= BatchFilter.RECOGNIZED_KEYS:
        raise gx_exceptions.BatchFilterError(
            f"""Unrecognized data_connector_query key(s):
"{str(data_connector_query_keys - BatchFilter.RECOGNIZED_KEYS)}" detected.
            """
        )
    custom_filter_function: Optional[Callable] = data_connector_query_dict.get(  # type: ignore[assignment]
        "custom_filter_function"
    )
    if custom_filter_function and not isinstance(custom_filter_function, Callable):  # type: ignore[arg-type]
        raise gx_exceptions.BatchFilterError(
            f"""The type of a custom_filter must be a function (Python "Callable").  The type given is
"{str(type(custom_filter_function))}", which is illegal.
            """
        )
    batch_filter_parameters: Optional[
        Union[dict, IDDict]
    ] = data_connector_query_dict.get(  # type: ignore[assignment]
        "batch_filter_parameters"
    )
    if batch_filter_parameters:
        if not isinstance(batch_filter_parameters, dict):
            raise gx_exceptions.BatchFilterError(
                f"""The type of batch_filter_parameters must be a dictionary (Python "dict").  The type given is
"{str(type(batch_filter_parameters))}", which is illegal.
                """
            )
        if not all(isinstance(key, str) for key in batch_filter_parameters.keys()):
            raise gx_exceptions.BatchFilterError(
                'All batch_filter_parameters keys must strings (Python "str").'
            )
        batch_filter_parameters = IDDict(batch_filter_parameters)
    index: Optional[
        Union[int, list, tuple, slice, str]
    ] = data_connector_query_dict.get(  # type: ignore[assignment]
        "index"
    )
    limit: Optional[int] = data_connector_query_dict.get("limit")  # type: ignore[assignment]
    if limit and (not isinstance(limit, int) or limit < 0):
        raise gx_exceptions.BatchFilterError(
            f"""The type of a limit must be an integer (Python "int") that is greater than or equal to 0.  The
type and value given are "{str(type(limit))}" and "{limit}", respectively, which is illegal.
            """
        )
    if index is not None and limit is not None:
        raise gx_exceptions.BatchFilterError(
            "Only one of index or limit, but not both, can be specified (specifying both is illegal)."
        )
    index = parse_batch_slice(batch_slice=index) if index is not None else None
    return BatchFilter(
        custom_filter_function=custom_filter_function,
        batch_filter_parameters=batch_filter_parameters,  # type: ignore[arg-type]
        index=index,
        limit=limit,
    )


class BatchFilter:
    RECOGNIZED_KEYS: set = {
        "custom_filter_function",
        "batch_filter_parameters",
        "index",
        "limit",
    }

    def __init__(
        self,
        custom_filter_function: Optional[Callable] = None,
        batch_filter_parameters: Optional[IDDict] = None,
        index: Optional[Union[int, slice]] = None,
        limit: Optional[int] = None,
    ) -> None:
        self._custom_filter_function = custom_filter_function
        self._batch_filter_parameters = batch_filter_parameters
        self._index = index
        self._limit = limit

    @property
    def custom_filter_function(self) -> Optional[Callable]:
        return self._custom_filter_function

    @property
    def batch_filter_parameters(self) -> Optional[IDDict]:
        return self._batch_filter_parameters

    @property
    def index(self) -> Optional[Union[int, slice]]:
        return self._index

    @property
    def limit(self) -> int:
        return self._limit  # type: ignore[return-value]

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "custom_filter_function": self._custom_filter_function,
            "batch_filter_parameters": self.batch_filter_parameters,
            "index": self.index,
            "limit": self.limit,
        }
        return str(doc_fields_dict)

    def select_from_data_connector_query(
        self, batch_definition_list: Optional[List[BatchDefinition]] = None
    ) -> List[BatchDefinition]:
        if batch_definition_list is None:
            return []
        filter_function: Callable
        if self.custom_filter_function:
            filter_function = self.custom_filter_function
        else:
            filter_function = self.best_effort_batch_definition_matcher()
        selected_batch_definitions: List[BatchDefinition]
        selected_batch_definitions = list(
            filter(
                lambda batch_definition: filter_function(
                    batch_identifiers=batch_definition.batch_identifiers,
                ),
                batch_definition_list,
            )
        )
        if len(selected_batch_definitions) == 0:
            return selected_batch_definitions

        if self.index is None:
            selected_batch_definitions = selected_batch_definitions[: self.limit]
        else:
            if isinstance(self.index, int):
                selected_batch_definitions = [selected_batch_definitions[self.index]]
            else:
                selected_batch_definitions = list(
                    itertools.chain.from_iterable(
                        [selected_batch_definitions[self.index]]
                    )
                )
        return selected_batch_definitions

    def best_effort_batch_definition_matcher(self) -> Callable:
        def match_batch_identifiers_to_batch_filter_params(
            batch_identifiers: dict,
        ) -> bool:
            if self.batch_filter_parameters:
                if not batch_identifiers:
                    return False

                for batch_filter_parameter, val in self.batch_filter_parameters.items():
                    if not (
                        batch_filter_parameter in batch_identifiers
                        and batch_identifiers[batch_filter_parameter] == val
                    ):
                        return False

            return True

        return match_batch_identifiers_to_batch_filter_params
